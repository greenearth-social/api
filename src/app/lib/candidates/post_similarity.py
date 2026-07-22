"""Post-similarity candidate generator.

Generates candidates by finding posts similar to a user's recent likes:

1. Query the ``likes`` index for the user's most recent liked posts.
2. Fetch those posts from the ``posts`` index to retrieve MiniLM L12 embeddings.
3. Average the embeddings into a single query vector.
4. Run a kNN nearest-neighbours search against the ``posts_recent`` index.
"""

import logging

from .base import CandidateGenerator, CandidateResult
from ..elasticsearch import fetch_recent_liked_post_uris, fetch_post_embeddings
from ..feed_debug import current_recorder
from ..embeddings import MINILM_L12_EMBEDDING_FIELD
from .es_candidates import knn_search_posts

logger = logging.getLogger(__name__)


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
        video_only: bool = False,
        exclude_uris: list[str] | None = None,
    ) -> CandidateResult:
        rec = current_recorder()

        # 1. Get recently liked post URIs
        liked_uris = await fetch_recent_liked_post_uris(es, user_did)

        if not liked_uris:
            logger.info("No likes found for user %s", user_did)
            if rec is not None:
                rec.record_user_features(self.name, [], 0)
            return CandidateResult(generator_name=self.name, candidates=[])

        # 2. Fetch embeddings for those posts
        embedding_pairs = await fetch_post_embeddings(es, liked_uris)

        if rec is not None:
            rec.record_user_features(self.name, liked_uris, len(embedding_pairs))

        if not embedding_pairs:
            logger.info(
                "No embeddings found for %d liked posts of user %s",
                len(liked_uris),
                user_did,
            )
            return CandidateResult(generator_name=self.name, candidates=[])

        # 3. Average the embedding vectors
        vectors = [embedding for _, embedding in embedding_pairs]
        avg_vector = average_vectors(vectors)

        # 4. kNN search for similar posts
        candidates = await knn_search_posts(
            es, avg_vector, num_candidates, search_field=MINILM_L12_EMBEDDING_FIELD,
            generator_name=self.name, video_only=video_only, exclude_uris=exclude_uris,
        )

        return CandidateResult(generator_name=self.name, candidates=candidates)
