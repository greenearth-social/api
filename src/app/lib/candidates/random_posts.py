"""Random-posts candidate generator.

Returns random recent posts from Elasticsearch using ``random_score``.
Useful as a simple baseline generator and as a low-correlation fallback.
"""

from ...models import CandidatePost
from .base import CandidateGenerator, CandidateResult
from ..elasticsearch import unwrap_es_response
from ..embeddings import encode_float32_b64


async def random_posts_search(
    es,
    num_candidates: int,
    generator_name: str | None = None,
    video_only: bool = False,
) -> list[CandidatePost]:
    """Fetch random posts from the ``posts`` index."""

    query = {
        "function_score": {
            "query": {
                "bool": {
                    "filter": [
                        f for f in [
                            {"term": {"contains_video": True}} if video_only else None,
                        ] if f is not None
                    ],
                }
            },
            "random_score": {},
            "boost_mode": "replace",
        }
    }

    resp = await es.search(index="posts", query=query, size=num_candidates)
    data = unwrap_es_response(resp)

    candidates: list[CandidatePost] = []
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
            CandidatePost(
                at_uri=src.get("at_uri"),
                content=src.get("content"),
                minilm_l12_embedding=encoded,
                score=hit.get("_score"),
                generator_name=generator_name,
            )
        )

    return candidates


class RandomPostsCandidateGenerator(CandidateGenerator):
    """Returns random posts independent of the requesting user."""

    @property
    def name(self) -> str:
        return "random_posts"

    async def generate(
        self,
        es,
        user_did: str,
        num_candidates: int = 100,
        video_only: bool = False,
    ) -> CandidateResult:
        candidates = await random_posts_search(
            es,
            num_candidates,
            generator_name=self.name,
            video_only=video_only,
        )
        return CandidateResult(generator_name=self.name, candidates=candidates)
