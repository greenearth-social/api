"""Post-similarity candidate generator.

Generates candidates by finding posts similar to a user's recent likes:

1. Query the ``likes`` index for the user's most recent liked posts.
2. Fetch those posts from the ``posts`` index to retrieve MiniLM L12 embeddings.
3. Average the embeddings into a single query vector.
4. Run a kNN nearest-neighbours search against the ``posts`` index.
"""

import logging

from elastic_transport import ObjectApiResponse

from .base import Candidate, CandidateGenerator, CandidateResult
from ..embeddings import encode_float32_b64

logger = logging.getLogger(__name__)

# How many recent likes to consider when building the query vector.
DEFAULT_LIKED_POSTS_LIMIT = 50


def unwrap_es_response(resp) -> dict:
    """Unwrap an Elasticsearch response (ObjectApiResponse or plain dict)."""
    if isinstance(resp, ObjectApiResponse):
        return resp.body
    if isinstance(resp, dict):
        return resp
    raise TypeError(f"Unexpected Elasticsearch response type: {type(resp)}")


async def fetch_recent_liked_post_uris(
    es,
    user_did: str,
    limit: int = DEFAULT_LIKED_POSTS_LIMIT,
) -> list[str]:
    """Return the AT URIs of posts the user most recently liked.

    Queries the ``likes`` index for documents where ``author_did`` matches
    *user_did*, sorted by ``created_at`` descending, and extracts the
    ``subject_uri`` field from each hit.
    """
    query = {
        "bool": {
            "filter": [{"term": {"author_did": user_did}}],
        }
    }

    resp = await es.search(
        index="likes",
        query=query,
        size=limit,
        sort=[{"created_at": "desc"}],
        _source=["subject_uri"],
    )

    data = unwrap_es_response(resp)
    uris: list[str] = []
    for hit in data.get("hits", {}).get("hits", []):
        uri = (hit.get("_source") or {}).get("subject_uri")
        if uri:
            uris.append(uri)
    return uris


async def fetch_post_embeddings(
    es,
    at_uris: list[str],
) -> list[list[float]]:
    """Fetch MiniLM L12 embeddings for a list of post AT URIs.

    Returns only the embeddings that were found and non-empty;
    posts without embeddings are silently skipped.
    """
    if not at_uris:
        return []

    query = {"terms": {"at_uri": at_uris}}

    resp = await es.search(
        index="posts",
        query=query,
        size=len(at_uris),
        _source=["embeddings.all_MiniLM_L12_v2"],
    )

    data = unwrap_es_response(resp)
    vectors: list[list[float]] = []
    for hit in data.get("hits", {}).get("hits", []):
        src = hit.get("_source") or {}
        emb = src.get("embeddings")
        if isinstance(emb, dict):
            vec = emb.get("all_MiniLM_L12_v2")
            if vec:
                vectors.append(vec)
    return vectors


def average_vectors(vectors: list[list[float]]) -> list[float]:
    """Compute the element-wise mean of a list of equal-length vectors."""
    if not vectors:
        raise ValueError("No vectors to average")

    dim = len(vectors[0])
    avg = [0.0] * dim
    for v in vectors:
        for i, val in enumerate(v):
            avg[i] += val
    n = len(vectors)
    return [x / n for x in avg]


async def knn_search_posts(
    es,
    query_vector: list[float],
    num_candidates: int,
) -> list[Candidate]:
    """Run a kNN search against the ``posts`` index and return candidates.

    Uses the ``embeddings.all_MiniLM_L12_v2`` field for nearest-neighbour
    matching.  Each hit is converted to a :class:`Candidate` with the ES
    score attached.
    """
    knn_query = {
        "bool": {
            "must": {
                "knn": {
                    "field": "embeddings.all_MiniLM_L12_v2",
                    "query_vector": query_vector,
                    "k": num_candidates,
                    "num_candidates": max(100, num_candidates * 10),
                }
            },
            "filter": [{"term": {"contains_video": True}}],
        }
    }

    resp = await es.search(index="posts", query=knn_query, size=num_candidates)
    data = unwrap_es_response(resp)

    candidates: list[Candidate] = []
    for hit in data.get("hits", {}).get("hits", []):
        src = hit.get("_source") or {}
        embeddings_obj = src.get("embeddings") or {}

        l12 = (
            embeddings_obj.get("all_MiniLM_L12_v2")
            if isinstance(embeddings_obj, dict)
            else None
        )

        encoded = None
        if l12 is not None:
            try:
                encoded = encode_float32_b64(l12)
            except Exception:
                encoded = None

        candidates.append(
            Candidate(
                at_uri=src.get("at_uri"),
                content=src.get("content"),
                minilm_l12_embedding=encoded,
                score=hit.get("_score"),
            )
        )
    return candidates


class PostSimilarityCandidateGenerator(CandidateGenerator):
    """Candidate generator based on cosine similarity of liked-post embeddings.

    Pipeline:
        user_did → recent likes → post embeddings → average → kNN search
    """

    @property
    def name(self) -> str:
        return "post_similarity"

    async def generate(
        self,
        es,
        user_did: str,
        num_candidates: int = 100,
    ) -> CandidateResult:
        # 1. Get recently liked post URIs
        liked_uris = await fetch_recent_liked_post_uris(es, user_did)

        if not liked_uris:
            logger.info("No likes found for user %s", user_did)
            return CandidateResult(generator_name=self.name, candidates=[])

        # 2. Fetch embeddings for those posts
        vectors = await fetch_post_embeddings(es, liked_uris)

        if not vectors:
            logger.info(
                "No embeddings found for %d liked posts of user %s",
                len(liked_uris),
                user_did,
            )
            return CandidateResult(generator_name=self.name, candidates=[])

        # 3. Average the embedding vectors
        avg_vector = average_vectors(vectors)

        # 4. kNN search for similar posts
        candidates = await knn_search_posts(es, avg_vector, num_candidates)

        return CandidateResult(generator_name=self.name, candidates=candidates)
