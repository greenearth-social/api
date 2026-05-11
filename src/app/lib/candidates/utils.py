from ...models import CandidatePost
from ..elasticsearch import unwrap_es_response
from ..embeddings import MINILM_L12_EMBEDDING_KEY, encode_float32_b64


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
    if l12 is not None:
        try:
            encoded = encode_float32_b64(l12)
        except Exception:
            encoded = None

    return CandidatePost(
        at_uri=src.get("at_uri"),
        content=src.get("content"),
        minilm_l12_embedding=encoded,
        score=hit.get("_score"),
        generator_name=generator_name,
    )
