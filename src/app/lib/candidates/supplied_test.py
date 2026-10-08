"""Tests for the caller-supplied (``external``) candidate generator."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from ...models import ExternalCandidate
from .supplied import SUPPLIED_CANDIDATES_INDEX, SuppliedCandidatesGenerator


def _hit(uri: str, *, contains_video: bool = False) -> dict:
    return {
        "_score": 1.0,
        "_source": {
            "at_uri": uri,
            "author_did": f"did:plc:{uri.rsplit('/', 1)[-1]}",
            "content": f"post {uri}",
            "contains_video": contains_video,
            "like_count": 3,
        },
    }


def _es_returning(*uris_and_flags):
    hits = [_hit(uri, contains_video=flag) for uri, flag in uris_and_flags]
    es = AsyncMock()
    es.search = AsyncMock(return_value={"hits": {"hits": hits}})
    return es


@pytest.mark.asyncio
async def test_keeps_caller_order_and_score_drops_unknown_uris():
    gen = SuppliedCandidatesGenerator(
        [
            ExternalCandidate(at_uri="at://a/post/1", score=0.9),
            ExternalCandidate(at_uri="at://a/post/missing", score=0.8),
            ExternalCandidate(at_uri="at://a/post/2"),
        ]
    )
    # ES returns hits in its own order; caller order must win.
    es = _es_returning(("at://a/post/2", False), ("at://a/post/1", False))

    result = await gen.generate(es, "did:plc:user", num_candidates=10)

    assert result.status == "success"
    assert [c.at_uri for c in result.candidates] == ["at://a/post/1", "at://a/post/2"]
    assert [c.score for c in result.candidates] == [0.9, None]
    assert all(c.generator_name == "external" for c in result.candidates)
    # Metadata comes from the index, not the caller.
    assert result.candidates[0].author_did == "did:plc:1"
    assert result.candidates[0].content == "post at://a/post/1"

    es.search.assert_awaited_once()
    kwargs = es.search.await_args.kwargs
    assert kwargs["index"] == SUPPLIED_CANDIDATES_INDEX
    assert kwargs["query"] == {
        "terms": {"at_uri": ["at://a/post/1", "at://a/post/missing", "at://a/post/2"]}
    }
    assert kwargs["size"] == 3


@pytest.mark.asyncio
async def test_excluded_uris_are_not_looked_up():
    gen = SuppliedCandidatesGenerator(
        [ExternalCandidate(at_uri="at://a/post/1"), ExternalCandidate(at_uri="at://a/post/2")]
    )
    es = _es_returning(("at://a/post/2", False))

    result = await gen.generate(es, "did:plc:user", exclude_uris=["at://a/post/1"])

    assert [c.at_uri for c in result.candidates] == ["at://a/post/2"]
    assert es.search.await_args.kwargs["query"] == {"terms": {"at_uri": ["at://a/post/2"]}}


@pytest.mark.asyncio
async def test_everything_excluded_skips_elasticsearch():
    gen = SuppliedCandidatesGenerator([ExternalCandidate(at_uri="at://a/post/1")])
    es = AsyncMock()

    result = await gen.generate(es, "did:plc:user", exclude_uris=["at://a/post/1"])

    assert result.status == "empty"
    assert result.reason == "all_excluded"
    es.search.assert_not_awaited()


@pytest.mark.asyncio
async def test_nothing_in_index_is_reported():
    gen = SuppliedCandidatesGenerator([ExternalCandidate(at_uri="at://a/post/1")])
    es = _es_returning()

    result = await gen.generate(es, "did:plc:user")

    assert result.status == "empty"
    assert result.reason == "not_in_index"


@pytest.mark.asyncio
async def test_video_only_filters_on_index_metadata():
    gen = SuppliedCandidatesGenerator(
        [ExternalCandidate(at_uri="at://a/post/1"), ExternalCandidate(at_uri="at://a/post/2")]
    )
    es = _es_returning(("at://a/post/1", False), ("at://a/post/2", True))

    result = await gen.generate(es, "did:plc:user", video_only=True)

    assert [c.at_uri for c in result.candidates] == ["at://a/post/2"]

    es = _es_returning(("at://a/post/1", False))
    result = await gen.generate(es, "did:plc:user", video_only=True)
    assert result.status == "empty"
    assert result.reason == "video_only_excluded"


@pytest.mark.asyncio
async def test_truncates_to_allocation_and_dedups_supplied_uris():
    gen = SuppliedCandidatesGenerator(
        [
            ExternalCandidate(at_uri="at://a/post/1", score=0.5),
            ExternalCandidate(at_uri="at://a/post/1", score=0.1),  # duplicate, dropped
            ExternalCandidate(at_uri="at://a/post/2"),
            ExternalCandidate(at_uri="at://a/post/3"),
        ]
    )
    assert gen.supplied_count == 3
    es = _es_returning(("at://a/post/1", False), ("at://a/post/2", False), ("at://a/post/3", False))

    result = await gen.generate(es, "did:plc:user", num_candidates=2)

    assert [c.at_uri for c in result.candidates] == ["at://a/post/1", "at://a/post/2"]
    assert result.candidates[0].score == 0.5
