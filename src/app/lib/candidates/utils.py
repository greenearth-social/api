from ...models import CandidatePost
from ..elasticsearch import (
    embedding_from_fields,
    post_has_embedding_source,
    unwrap_es_response,
)
from ..embeddings import MINILM_L12_EMBEDDING_FIELD, MINILM_L12_EMBEDDING_KEY, encode_float32_b64
from ..topic_scores import politics_score_from_source

# Fields every candidate generator should pull from ES via `_source`.
# Critically, this does NOT include the 384-dim embedding array, even
# though MMR and the heavy ranker need it downstream. A kNN search
# at k=250 with embeddings in _source returns ~1.8 MB; without them it
# returns ~50 KB. A generator whose hits are exactly its candidates adds
# CANDIDATE_EMBEDDING_DOCVALUE_FIELDS instead (doc values, not _source);
# the rest are filled by hydrate_posts, per generator and from the
# post-features cache where possible.
CANDIDATE_SOURCE_FIELDS = [
    "at_uri",
    "author_did",
    "content",
    "contains_video",       # used for video_only filtering
    "contains_images",      # media metadata (feed debugging)
    "image_count",          # media metadata (feed debugging)
    "video_count",          # media metadata (feed debugging)
    "external_embed",       # link embed metadata (feed debugging)
    "like_count",           # used for popularity scoring
    "topic_scores",         # used for politics multiplier
    # Perspective scores computed at ingest (ingex), so the perspective ranker
    # does not have to call the API on the serving path. Two fields, not one:
    # a post stamped with `perspective_scored_at` and no score is permanently
    # unscorable (no text, or a language the API declines to rate), which is
    # different from one that has simply never been scored. See
    # lib/perspective.score_candidates.
    "combined_perspective_score",
    "perspective_scored_at",
]


# Pass as ``docvalue_fields`` to a generator query whose hits are the
# generator's final candidates, so they arrive with the MiniLM embedding the
# ranker needs and skip a later hydration lookup. Not for queries that
# overfetch: at ~4.5 KB per hit the vector dominates the response size.
CANDIDATE_EMBEDDING_DOCVALUE_FIELDS = [MINILM_L12_EMBEDDING_FIELD]


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
    if l12 is None:
        # Generators that request CANDIDATE_EMBEDDING_DOCVALUE_FIELDS get the
        # vector from doc values rather than _source (see embedding_from_fields).
        l12 = embedding_from_fields(hit)

    encoded = None
    if l12 is not None and post_has_embedding_source(src):
        try:
            encoded = encode_float32_b64(l12)
        except Exception:
            encoded = None

    external_embed = src.get("external_embed")
    external_uri = (
        external_embed.get("uri") if isinstance(external_embed, dict) else None
    )

    return CandidatePost(
        author_did=src.get("author_did"),
        at_uri=src.get("at_uri"),
        content=src.get("content"),
        minilm_l12_embedding=encoded,
        score=hit.get("_score"),
        generator_name=generator_name,
        contains_images=src.get("contains_images"),
        contains_video=src.get("contains_video"),
        image_count=src.get("image_count"),
        video_count=src.get("video_count"),
        external_uri=external_uri,
        like_count=src.get("like_count"),
        politics_score=politics_score_from_source(src),
        combined_perspective_score=src.get("combined_perspective_score"),
        perspective_scored_at=src.get("perspective_scored_at"),
    )
