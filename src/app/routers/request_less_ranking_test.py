"""RequestLess adjustment through the shared ranker and its public surfaces."""

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient

from ..documents import UserDocument
from ..feeds import FEEDS
from ..lib import metrics, posthog_client, slate_pipeline
from ..lib.embeddings import encode_float32_b64
from ..lib.feed_cache import InMemoryFeedCache
from ..lib.firebase_auth import verify_firebase_auth
from ..lib.rankers import heavy_ranker
from ..lib.request_context import reset_traffic, set_traffic
from ..lib.request_less_history import RequestLessHistory, RequestLessHistoryItem
from ..lib.user_history_cache import UserHistory, UserHistoryItem
from ..main import app
from ..models import (
    CandidateGenerateResult,
    CandidatePost,
    FeedCursor,
    RankModelSpec,
    RankPredictRequest,
)
from ..security import verify_api_key
from . import slate, xrpc

USER_DID = "did:plc:request-less-user"
FEED_URI = "at://did:web:test.example/app.bsky.feed.generator/your-feed"
URIS = [f"at://post/{name}" for name in ("a", "b", "c")]
NOW = datetime(2026, 10, 27, tzinfo=UTC)


@pytest.fixture
def ranking_surfaces(monkeypatch):
    candidates = [
        CandidatePost(
            at_uri=uri,
            author_did=f"did:plc:author-{name}",
            content=f"Post {name}",
            minilm_l12_embedding=encode_float32_b64([float(index), 1.0]),
            score=score,
        )
        for index, (name, uri, score) in enumerate(
            zip(("a", "b", "c"), URIS, (0.8, 0.6, 0.4), strict=True)
        )
    ]
    flag = {"enabled": True}
    flags = MagicMock()
    flags.get_flag.side_effect = lambda name: (
        flag["enabled"] if name == posthog_client.REQUEST_LESS_HEAVY_RANKER_FLAG else False
    )
    posthog = MagicMock()
    posthog.evaluate_flags.return_value = flags
    monkeypatch.setattr(posthog_client, "_posthog_client", posthog)
    monkeypatch.setattr(metrics, "_metric_collector", None)
    monkeypatch.setenv("GE_FEED_CONTEXT_SECRET", "request-less-integration-secret")
    monkeypatch.setattr(app.state, "es", AsyncMock(), raising=False)
    monkeypatch.setattr(app.state, "firestore", MagicMock(), raising=False)
    cache = InMemoryFeedCache()
    monkeypatch.setattr(app.state, "feed_cache", cache, raising=False)
    monkeypatch.setitem(app.dependency_overrides, verify_api_key, lambda: "test-key")
    monkeypatch.setitem(
        app.dependency_overrides, slate.require_user_config_scope, lambda: "test-key"
    )
    monkeypatch.setitem(app.dependency_overrides, verify_firebase_auth, lambda: "request-less-user")
    monkeypatch.setattr(xrpc, "verify_auth_header", AsyncMock(return_value=USER_DID))
    monkeypatch.setattr(xrpc, "get_user", AsyncMock(return_value=UserDocument(user_did=USER_DID)))
    monkeypatch.setattr(xrpc, "_generation_exclusions", AsyncMock(return_value=[]))
    monkeypatch.setattr(xrpc, "get_recent_seen_uris", AsyncMock(return_value=[]))
    monkeypatch.setattr(xrpc, "get_recent_discarded_uris", AsyncMock(return_value=[]))
    monkeypatch.setattr(xrpc, "claim_accepted_feed_slate", AsyncMock(return_value=None))
    monkeypatch.setattr(xrpc, "_write_feed_snapshot_background", AsyncMock())
    monkeypatch.setattr(xrpc, "_spawn_background", lambda coroutine: coroutine.close())
    monkeypatch.setattr(slate, "_spawn_background", lambda coroutine: coroutine.close())
    monkeypatch.setattr(slate, "claim_api_preview", AsyncMock(return_value=(None, "none")))
    monkeypatch.setattr(slate, "get_api_slate", AsyncMock(return_value=None))
    monkeypatch.setattr(slate, "save_api_slate", AsyncMock())
    monkeypatch.setattr(slate, "delete_api_slate", AsyncMock())
    monkeypatch.setitem(
        FEEDS,
        "your-feed",
        FEEDS["your-feed"].model_copy(
            update={
                "rank_request_template": RankPredictRequest(
                    candidates=[],
                    user_did=USER_DID,
                    models=[RankModelSpec(name="heavy_ranker", weight=1.0)],
                ),
                "diversify": False,
                "min_rank_score": None,
                "min_mmr_score": None,
                "max_render_share": None,
                "pinned_post_uri": None,
                "explore_pinned_post_uri": None,
                "returning_pinned_post_uri": None,
                "survey_post_uri": None,
                "favorite_video_post_uri": None,
            }
        ),
    )
    generate = AsyncMock(return_value=CandidateGenerateResult(candidates=candidates))
    monkeypatch.setattr(slate_pipeline, "run_generate", generate)
    monkeypatch.setattr(
        slate_pipeline, "hydrate_posts", AsyncMock(side_effect=lambda es, items: items)
    )
    monkeypatch.setattr(
        heavy_ranker, "get_inference_settings", lambda: ("https://inference", "secret")
    )
    monkeypatch.setattr(
        heavy_ranker,
        "fetch_user_history_features",
        AsyncMock(
            return_value=UserHistory(
                [
                    UserHistoryItem(
                        "at://history/liked", NOW.isoformat(), [1.0, 0.0], "did:plc:liked", 3
                    ),
                ]
            )
        ),
    )
    request_less = AsyncMock(
        return_value=RequestLessHistory(
            [
                RequestLessHistoryItem("at://history/less", NOW, [-1.0, 0.0], "did:plc:less", 4),
            ]
        )
    )
    monkeypatch.setattr(heavy_ranker, "fetch_request_less_history_features", request_less)

    async def predict(history_embeddings, *args, **kwargs):
        scores = (1.0, 0.0, 1.0) if history_embeddings == [[-1.0, 0.0]] else (0.8, 0.6, 0.4)
        by_author = dict(
            zip(
                ("did:plc:author-a", "did:plc:author-b", "did:plc:author-c"),
                scores,
                strict=True,
            )
        )
        return [by_author[author] for author in args[4]]

    inference = AsyncMock(side_effect=predict)
    monkeypatch.setattr(heavy_ranker, "predict_heavy_ranker_single_user", inference)
    xrpc._clear_initial_request_cache()
    traffic_token = set_traffic("real")
    try:
        yield SimpleNamespace(
            client=TestClient(app),
            candidates=candidates,
            flag=flag,
            posthog=posthog,
            cache=cache,
            inference=inference,
            request_less=request_less,
            generate=generate,
        )
    finally:
        reset_traffic(traffic_token)
        xrpc._clear_initial_request_cache()


def _render_surface(state, surface, *, limit=9, cursor=None):
    if surface == "direct":
        response = state.client.post(
            "/rank/predict",
            json={
                "user_did": USER_DID,
                "models": [{"name": "heavy_ranker", "weight": 1.0}],
                "candidates": [candidate.model_dump(mode="json") for candidate in state.candidates],
            },
        )
        assert response.status_code == 200, response.text
        items = response.json()["rankings"]
        return SimpleNamespace(items=items, cursor=None)
    if surface in {"slate", "slate_preview"}:
        body = {
            "feed_name": "partner-feed",
            "user_did": USER_DID,
            "limit": limit,
            "generators": [{"name": "popularity", "weight": 1.0}],
            "rankers": [{"name": "heavy_ranker", "weight": 1.0}],
            "diversify": False,
            "preview": surface == "slate_preview",
        }
        if cursor is not None:
            body = {
                "feed_name": "partner-feed",
                "user_did": USER_DID,
                "limit": limit,
                "cursor": cursor,
            }
        response = state.client.post("/slate/generate", json=body)
        assert response.status_code == 200, response.text
        return SimpleNamespace(items=response.json()["items"], cursor=response.json()["cursor"])
    if surface == "xrpc":
        params = {"feed": FEED_URI, "limit": limit}
        if cursor is not None:
            params["cursor"] = cursor
        response = state.client.get("/xrpc/app.bsky.feed.getFeedSkeleton", params=params)
        assert response.status_code == 200, response.text
        next_cursor = response.json()["cursor"]
        cached = state.cache._docs[FeedCursor.decode(next_cursor).id]
        by_uri = {item.at_uri: item.model_dump() for item in cached.items_meta}
        return SimpleNamespace(
            items=[by_uri[item["post"]] for item in response.json()["feed"]],
            cursor=next_cursor,
        )
    assert surface == "settings_preview"
    response = state.client.post("/api/feeds/your-feed/preview", json={})
    assert response.status_code == 200, response.text
    cached = state.cache._docs[response.json()["request_id"]]
    return SimpleNamespace(items=[item.model_dump() for item in cached.items_meta], cursor=None)


@pytest.mark.parametrize(
    "surface", ["direct", "slate", "slate_preview", "xrpc", "settings_preview"]
)
@pytest.mark.parametrize("enabled", [False, True])
def test_request_less_adjustment_is_shared_by_all_ranking_surfaces(
    ranking_surfaces, surface, enabled
):
    state = ranking_surfaces
    state.flag["enabled"] = enabled

    result = _render_surface(state, surface)

    expected_uris = [URIS[1], URIS[0], URIS[2]] if enabled else URIS
    # The shared pipeline floors the final combined score after signed ranker
    # contributions have been combined.
    expected_scores = [0.6, 0.05, 0.0] if enabled else [0.8, 0.6, 0.4]
    assert [item["at_uri"] for item in result.items] == expected_uris
    assert [item["rank_score"] for item in result.items] == pytest.approx(expected_scores)
    assert state.inference.await_count == (2 if enabled else 1)
    if enabled:
        state.request_less.assert_awaited_once_with(USER_DID)
    else:
        state.request_less.assert_not_awaited()
    flag_calls = [
        call
        for call in state.posthog.evaluate_flags.call_args_list
        if call.kwargs["flag_keys"] == [posthog_client.REQUEST_LESS_HEAVY_RANKER_FLAG]
    ]
    assert len(flag_calls) == 1
    assert flag_calls[0].args == (USER_DID,)


@pytest.mark.parametrize("surface", ["slate", "xrpc"])
def test_cached_pages_keep_the_generated_ranking_without_rechecking_request_less(
    ranking_surfaces, surface
):
    state = ranking_surfaces
    extra_uris = []
    if surface == "xrpc":
        # Initial XRPC requests smaller than nine posts are Explore previews,
        # which intentionally have no pagination state.
        extra_uris = [f"at://post/extra-{index}" for index in range(7)]
        state.candidates.extend(
            state.candidates[1].model_copy(update={"at_uri": uri}) for uri in extra_uris
        )
        state.generate.return_value = CandidateGenerateResult(candidates=state.candidates)
    first = _render_surface(state, surface, limit=9 if surface == "xrpc" else 2)
    assert [item["at_uri"] for item in first.items] == [URIS[1], *extra_uris, URIS[0]]
    assert state.inference.await_count == 2
    state.flag["enabled"] = False

    second = _render_surface(state, surface, limit=2, cursor=first.cursor)

    assert [item["at_uri"] for item in second.items] == [URIS[2]]
    assert second.items[0]["rank_score"] == 0.0
    assert state.inference.await_count == 2
    state.request_less.assert_awaited_once_with(USER_DID)
    state.generate.assert_awaited_once()
