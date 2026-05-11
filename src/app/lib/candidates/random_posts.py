"""Random-posts candidate generator.

Returns random recent posts from Elasticsearch using ``random_score``.
Useful as a simple baseline generator and as a low-correlation fallback.
"""

from ...models import CandidatePost
from .base import CandidateGenerator, CandidateResult
from .utils import candidate_posts_from_es_response


async def random_posts_search(
    es,
    num_candidates: int,
    generator_name: str | None = None,
    video_only: bool = False,
    exclude_uris: list[str] | None = None,
) -> list[CandidatePost]:
    """Fetch random posts from the ``posts`` index."""

    filters: list[dict] = []
    if video_only:
        filters.append({"term": {"contains_video": True}})

    must_not: list[dict] = []
    if exclude_uris:
        must_not.append({"terms": {"at_uri": exclude_uris}})

    query = {
        "function_score": {
            "query": {
                "bool": {
                    "filter": filters,
                    **("must_not" and {"must_not": must_not} if must_not else {}),
                }
            },
            "random_score": {},
            "boost_mode": "replace",
        }
    }

    resp = await es.search(index="posts", query=query, size=num_candidates)
    return candidate_posts_from_es_response(resp, generator_name=generator_name)


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
        exclude_uris: list[str] | None = None,
    ) -> CandidateResult:
        candidates = await random_posts_search(
            es,
            num_candidates,
            generator_name=self.name,
            video_only=video_only,
            exclude_uris=exclude_uris,
        )
        return CandidateResult(generator_name=self.name, candidates=candidates)
