from ...models import CandidatePost
from ..elasticsearch import post_has_embedding_source, unwrap_es_response
from ..embeddings import MINILM_L12_EMBEDDING_KEY, encode_float32_b64


# Fields every candidate generator should pull from ES via `_source`.
# Critically, this does NOT include the 384-dim embedding array, even
# though MMR and the two-tower ranker need it downstream. A kNN search
# at k=250 with embeddings in _source returns ~1.8 MB; without them it
# returns ~50 KB. We refetch embeddings in one batched call after
# dedup, against the much smaller set of candidates that actually make
# it through.
CANDIDATE_SOURCE_FIELDS = [
    "at_uri",
    "author_did",
    "content",
    "thread_parent_post",   # used for Python-side reply filtering
    "contains_video",       # used for video_only filtering
]


def candidate_posts_from_es_response(
    resp,
    generator_name: str | None = None,
) -> list[CandidatePost]:
    data = unwrap_es_response(resp)
    return [
        candidate_post_from_hit(hit, generator_name=generator_name)
        for hit in data.get("hits", {}).get("hits", [])
    ]


def candidate_post_from_hit(
    hit: dict,
    generator_name: str | None = None,
) -> CandidatePost:
    src = hit.get("_source") or {}
    embeddings_obj = src.get("embeddings") or {}

    l12 = (
        embeddings_obj.get(MINILM_L12_EMBEDDING_KEY)
        if isinstance(embeddings_obj, dict)
        else None
    )

    encoded = None
    if l12 is not None and post_has_embedding_source(src):
        try:
            encoded = encode_float32_b64(l12)
        except Exception:
            encoded = None

    return CandidatePost(
        author_did=src.get("author_did"),
        at_uri=src.get("at_uri"),
        content=src.get("content"),
        minilm_l12_embedding=encoded,
        score=hit.get("_score"),
        generator_name=generator_name,
    )
