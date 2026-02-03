from pydantic import BaseModel, Field


class GreenEarthPost(BaseModel):
    """Represents a post returned by any search method."""
    at_uri: str | None = Field(
        None, description="The AT URI of the post (e.g. at://...)")
    content: str | None = Field(None, description="The post text content")
    minilm_l12_embedding: str | None = Field(
        None, description="Base64-encoded float32 array (384 floats)"
    )
