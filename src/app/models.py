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
