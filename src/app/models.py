from pydantic import BaseModel, Field


class CandidatePost(BaseModel):
    """A post returned by search or candidate generation."""

    at_uri: str | None = Field(
        None, description="The AT URI of the post (e.g. at://...)")
    content: str | None = Field(None, description="The post text content")
    minilm_l12_embedding: str | None = Field(
        None, description="Base64-encoded float32 MiniLM L12 embedding (384-d)"
    )
    score: float | None = Field(
        None, description="Relevance score (e.g. from ES or a model)"
    )
    generator_name: str | None = Field(
        None, description="Name of the candidate generator that produced this post"
    )


class FeedConfig(BaseModel):
    """Configuration for a single published feed.

    ``gen_request_template`` holds the generator pipeline spec using the same
    shape as ``CandidateGenerateRequest``.  Session-specific fields
    (``user_did``, ``num_candidates``) are filled in at request time.
    """

    display_name: str
    description: str = ""
    gen_request_template: "CandidateGenerateRequest"  # noqa: F821 — forward ref resolved at import time


# Resolve the forward reference once CandidateGenerateRequest is available.
# Callers that need FeedConfig with gen_request_template must ensure
# CandidateGenerateRequest has been imported first (feeds.py does this).
def _rebuild_feed_config() -> None:
    from .lib.candidates.generate import CandidateGenerateRequest  # noqa: F811
    FeedConfig.model_rebuild()
