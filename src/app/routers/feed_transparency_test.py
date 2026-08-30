"""Tests for feed-transparency API endpoints."""

from __future__ import annotations

from collections.abc import Generator
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from ..documents import (
    DiversificationMeta,
    FeedCacheDocument,
    FeedPreferencesDocument,
    FeedSnapshotDocument,
    GeneratorDiagnostic,
    GeneratorMeta,
    ModelScoreMeta,
    PipelineItemMeta,
)
from ..lib.firestore import StaleFeedPreviewError
from ..main import app

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def client() -> Generator[TestClient]:
    """Client with Firebase auth bypassed and Firestore mocked on app state."""
    from ..lib.firebase_auth import verify_firebase_auth

    app.dependency_overrides[verify_firebase_auth] = lambda: "test-user"
    app.state.firestore = MagicMock()
    app.state.id_resolver = MagicMock()
    client = TestClient(app)
    yield client
    app.dependency_overrides.pop(verify_firebase_auth, None)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _snapshot_doc(
    request_id: str = "req-abc",
    feed_name: str = "your-feed",
    generated_at: datetime | None = None,
    items_meta: list[PipelineItemMeta] | None = None,
    generator_legend: list[GeneratorMeta] | None = None,
    ranker_model: str | None = "two_tower",
    diversify: bool = True,
    **overrides,
) -> FeedSnapshotDocument:
    now = generated_at or datetime(2026, 7, 12, 15, 30, tzinfo=UTC)
    meta = items_meta or [
        PipelineItemMeta(
            at_uri="at://did:plc:author/app.bsky.feed.post/post1",
            rank=1,
            rank_score=0.92,
            after_rank_position=1,
            generators=[GeneratorMeta(name="two_tower", score=0.85)],
            model_scores=[ModelScoreMeta(name="two_tower", weight=1.0, score=0.92)],
            diversification=DiversificationMeta(
                relevance=0.95, score=0.80, author_penalty=0.0, content_penalty=0.0
            ),
        )
    ]
    legend = generator_legend or [GeneratorMeta(name="two_tower", weight=1.0)]

    defaults = dict(
        request_id=request_id,
        items=[m.at_uri for m in meta],
        feed_name=feed_name,
        generated_at=now,
        expires_at=now + timedelta(minutes=15),
        ranker_model=ranker_model,
        diversify=diversify,
        generator_legend=legend,
        items_meta=meta,
    )
    defaults.update(overrides)
    return FeedSnapshotDocument(**defaults)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# GET /api/feeds
# ---------------------------------------------------------------------------


@patch("app.routers.feed_transparency.get_recent_feed_snapshots")
def test_list_feeds_returns_summaries(mock_query, client):
    mock_query.return_value = [
        _snapshot_doc(
            request_id="req-1",
            api_release_sha="api-sha-1",
            generated_at=datetime.now(UTC),
            items=["at://a"],
            items_meta=[
                PipelineItemMeta(at_uri="at://a", rank=1, rank_score=1.0, after_rank_position=1)
            ],
        ),
        _snapshot_doc(
            request_id="req-2",
            generated_at=datetime.now(UTC) - timedelta(minutes=5),
            items=["at://b"],
            items_meta=[
                PipelineItemMeta(at_uri="at://b", rank=1, rank_score=1.0, after_rank_position=1)
            ],
        ),
    ]

    response = client.get("/api/feeds")
    assert response.status_code == 200
    data = response.json()
    assert len(data["feeds"]) == 2
    assert data["feeds"][0]["request_id"] == "req-1"
    assert data["feeds"][0]["feed_name"] == "your-feed"
    assert data["feeds"][0]["api_release_sha"] == "api-sha-1"


@patch("app.routers.feed_transparency.get_recent_feed_snapshots")
def test_list_feeds_queries_last_24_hours_with_100_document_limit(mock_query, client):
    mock_query.return_value = []
    earliest_cutoff = datetime.now(UTC) - timedelta(hours=24)

    response = client.get("/api/feeds")

    latest_cutoff = datetime.now(UTC) - timedelta(hours=24)
    assert response.status_code == 200
    assert mock_query.await_args is not None
    assert earliest_cutoff <= mock_query.await_args.kwargs["cutoff"] <= latest_cutoff
    assert mock_query.await_args.kwargs["limit"] == 100
    assert mock_query.await_args.kwargs["raise_on_error"] is True


@patch("app.routers.feed_transparency.get_recent_feed_snapshots")
def test_list_feeds_does_not_report_query_failures_as_empty_history(mock_query, client):
    mock_query.side_effect = RuntimeError("query unavailable")

    with pytest.raises(RuntimeError, match="query unavailable"):
        client.get("/api/feeds")


@patch("app.routers.feed_transparency.get_recent_feed_snapshots")
def test_list_feeds_returns_public_feeds_without_a_query_filter(mock_query, client):
    """The broad query is filtered to the public observability pages in memory."""
    now = datetime.now(UTC)
    mock_query.return_value = [
        _snapshot_doc(
            request_id="req-1", generated_at=now, items=["at://a"], feed_name="your-feed"
        ),
        _snapshot_doc(
            request_id="req-2",
            generated_at=now - timedelta(minutes=1),
            items=["at://b"],
            feed_name="popularity",
        ),
    ]

    response = client.get("/api/feeds")

    assert response.status_code == 200
    assert [f["feed_name"] for f in response.json()["feeds"]] == ["your-feed"]
    # No feed_name filter reaches the query.
    assert "feed_name" not in mock_query.call_args.kwargs


@patch("app.routers.feed_transparency.get_recent_feed_snapshots")
def test_list_feeds_maps_published_rkeys_to_public_feed_pages(mock_query, client):
    now = datetime.now(UTC)
    mock_query.return_value = [
        _snapshot_doc(request_id="req-1", generated_at=now, items=["at://a"], feed_name="a0-yf"),
        _snapshot_doc(
            request_id="req-2",
            generated_at=now - timedelta(minutes=1),
            items=["at://b"],
            feed_name="fd-bof",
        ),
        _snapshot_doc(
            request_id="req-3",
            generated_at=now - timedelta(minutes=2),
            items=["at://c"],
            feed_name="67-r",
        ),
    ]

    response = client.get("/api/feeds")

    assert response.status_code == 200
    assert [f["feed_name"] for f in response.json()["feeds"]] == [
        "your-feed",
        "best-of-friends",
        "random",
    ]


@patch("app.routers.feed_transparency.get_recent_feed_snapshots")
def test_list_feeds_empty(mock_query, client):
    mock_query.return_value = []

    response = client.get("/api/feeds")
    assert response.status_code == 200
    assert response.json()["feeds"] == []


def test_list_feeds_returns_401_without_auth():
    from ..lib.firebase_auth import verify_firebase_auth

    app.dependency_overrides.pop(verify_firebase_auth, None)
    try:
        client = TestClient(app)
        response = client.get("/api/feeds")
        assert response.status_code == 401
    finally:
        app.dependency_overrides.setdefault(verify_firebase_auth, lambda: "test-user")


@patch("app.routers.feed_transparency.get_recent_feed_snapshots")
def test_list_feeds_collapses_identical_snapshots_and_keeps_newest(mock_query, client):
    now = datetime.now(UTC)
    newer = _snapshot_doc(
        request_id="req-1",
        generated_at=now,
        items=["at://a", "at://b"],
        items_meta=[
            PipelineItemMeta(at_uri="at://a", rank=1, rank_score=1.0, after_rank_position=1),
            PipelineItemMeta(at_uri="at://b", rank=2, rank_score=0.9, after_rank_position=2),
        ],
    )
    older = _snapshot_doc(
        request_id="req-2",
        generated_at=now - timedelta(minutes=5),
        items=["at://a", "at://b"],
        items_meta=[
            PipelineItemMeta(at_uri="at://a", rank=1, rank_score=1.0, after_rank_position=1),
            PipelineItemMeta(at_uri="at://b", rank=2, rank_score=0.9, after_rank_position=2),
        ],
    )
    mock_query.return_value = [newer, older]

    response = client.get("/api/feeds")
    data = response.json()
    assert [feed["request_id"] for feed in data["feeds"]] == ["req-1"]


@patch("app.routers.feed_transparency.get_recent_feed_snapshots")
def test_list_feeds_hides_empty_bootstrap_until_first_populated_snapshot(mock_query, client):
    now = datetime.now(UTC)
    mock_query.return_value = [
        _snapshot_doc(
            request_id="empty-new",
            generated_at=now,
            items=[],
            items_meta=[],
        ),
        _snapshot_doc(
            request_id="empty-old",
            generated_at=now - timedelta(minutes=5),
            items=[],
            items_meta=[],
        ),
    ]

    response = client.get("/api/feeds")

    assert response.status_code == 200
    assert response.json()["feeds"] == []


@patch("app.routers.feed_transparency.get_recent_feed_snapshots")
def test_list_feeds_replaces_bootstrap_empty_with_first_populated_snapshot(mock_query, client):
    now = datetime.now(UTC)
    mock_query.return_value = [
        _snapshot_doc(
            request_id="first-populated",
            generated_at=now,
            items=["at://first/post"],
        ),
        _snapshot_doc(
            request_id="bootstrap-empty",
            generated_at=now - timedelta(minutes=5),
            items=[],
            items_meta=[],
            generator_diagnostics=[
                GeneratorDiagnostic(
                    name="two_tower",
                    weight=1,
                    requested_count=100,
                    returned_count=10,
                    contributed_count=0,
                    status="empty",
                    reason="ranking_removed_all",
                )
            ],
        ),
    ]

    response = client.get("/api/feeds")

    assert response.status_code == 200
    assert [feed["request_id"] for feed in response.json()["feeds"]] == ["first-populated"]


@patch("app.routers.feed_transparency.get_recent_feed_snapshots")
def test_list_feeds_hides_empty_refresh_after_first_populated_snapshot(mock_query, client):
    now = datetime.now(UTC)
    mock_query.return_value = [
        _snapshot_doc(
            request_id="real-empty",
            generated_at=now,
            items=[],
            items_meta=[],
        ),
        _snapshot_doc(
            request_id="first-populated",
            generated_at=now - timedelta(minutes=5),
            items=["at://first/post"],
        ),
        _snapshot_doc(
            request_id="bootstrap-empty",
            generated_at=now - timedelta(minutes=10),
            items=[],
            items_meta=[],
        ),
    ]

    response = client.get("/api/feeds")

    assert response.status_code == 200
    assert [feed["request_id"] for feed in response.json()["feeds"]] == ["first-populated"]


@patch("app.routers.feed_transparency.get_recent_feed_snapshots")
def test_list_feeds_preserves_same_posts_in_different_order(mock_query, client):
    now = datetime.now(UTC)
    mock_query.return_value = [
        _snapshot_doc(
            request_id="req-1",
            generated_at=now,
            items=["at://a", "at://b"],
        ),
        _snapshot_doc(
            request_id="req-2",
            generated_at=now - timedelta(minutes=5),
            items=["at://b", "at://a"],
        ),
    ]

    response = client.get("/api/feeds")

    assert [feed["request_id"] for feed in response.json()["feeds"]] == [
        "req-1",
        "req-2",
    ]


@patch("app.routers.feed_transparency.get_recent_feed_snapshots")
def test_list_feeds_preserves_identical_posts_from_different_feeds(mock_query, client):
    now = datetime.now(UTC)
    mock_query.return_value = [
        _snapshot_doc(
            request_id="req-1",
            generated_at=now,
            feed_name="your-feed",
            items=["at://a", "at://b"],
        ),
        _snapshot_doc(
            request_id="req-2",
            generated_at=now - timedelta(minutes=5),
            feed_name="best-of-friends",
            items=["at://a", "at://b"],
        ),
    ]

    response = client.get("/api/feeds")

    assert [feed["request_id"] for feed in response.json()["feeds"]] == [
        "req-1",
        "req-2",
    ]


@patch("app.routers.feed_transparency.get_recent_feed_snapshots")
def test_list_feeds_newest_first_order(mock_query, client):
    now = datetime.now(UTC)
    newest = _snapshot_doc(
        request_id="req-3",
        generated_at=now,
        items=["at://c"],
        items_meta=[
            PipelineItemMeta(at_uri="at://c", rank=1, rank_score=1.0, after_rank_position=1)
        ],
    )
    middle = _snapshot_doc(
        request_id="req-2",
        generated_at=now - timedelta(minutes=3),
        items=["at://b"],
        items_meta=[
            PipelineItemMeta(at_uri="at://b", rank=1, rank_score=1.0, after_rank_position=1)
        ],
    )
    oldest = _snapshot_doc(
        request_id="req-1",
        generated_at=now - timedelta(minutes=6),
        items=["at://a"],
        items_meta=[
            PipelineItemMeta(at_uri="at://a", rank=1, rank_score=1.0, after_rank_position=1)
        ],
    )
    mock_query.return_value = [newest, middle, oldest]

    response = client.get("/api/feeds")
    data = response.json()
    assert len(data["feeds"]) == 3
    assert data["feeds"][0]["request_id"] == "req-3"
    assert data["feeds"][1]["request_id"] == "req-2"
    assert data["feeds"][2]["request_id"] == "req-1"


@patch("app.routers.feed_transparency.get_recent_feed_snapshots")
def test_list_feeds_preserves_fully_overlapping_middle_snapshot(mock_query, client):
    now = datetime.now(UTC)
    newest = _snapshot_doc(
        request_id="req-3",
        generated_at=now,
        items=["at://a", "at://b", "at://c"],
        items_meta=[
            PipelineItemMeta(at_uri="at://a", rank=1, rank_score=1.0, after_rank_position=1),
            PipelineItemMeta(at_uri="at://b", rank=2, rank_score=0.9, after_rank_position=2),
            PipelineItemMeta(at_uri="at://c", rank=3, rank_score=0.8, after_rank_position=3),
        ],
    )
    middle = _snapshot_doc(
        request_id="req-2",
        generated_at=now - timedelta(minutes=3),
        items=["at://a", "at://b"],
        items_meta=[
            PipelineItemMeta(at_uri="at://a", rank=1, rank_score=1.0, after_rank_position=1),
            PipelineItemMeta(at_uri="at://b", rank=2, rank_score=0.9, after_rank_position=2),
        ],
    )
    oldest = _snapshot_doc(
        request_id="req-1",
        generated_at=now - timedelta(minutes=6),
        items=["at://d"],
        items_meta=[
            PipelineItemMeta(at_uri="at://d", rank=1, rank_score=1.0, after_rank_position=1)
        ],
    )
    mock_query.return_value = [newest, middle, oldest]

    response = client.get("/api/feeds")
    data = response.json()
    assert len(data["feeds"]) == 3
    assert data["feeds"][0]["request_id"] == "req-3"
    assert data["feeds"][1]["request_id"] == "req-2"
    assert data["feeds"][2]["request_id"] == "req-1"


# ---------------------------------------------------------------------------
# GET /api/feeds/{request_id}
# ---------------------------------------------------------------------------


@patch("app.routers.feed_transparency.generate_feed_preview", new_callable=AsyncMock)
def test_create_feed_preview_accepts_unsaved_preferences(mock_generate, client):
    snapshot = _snapshot_doc(request_id="preview-1")
    mock_generate.return_value = snapshot

    response = client.post(
        "/api/feeds/your-feed/preview",
        json={"freshness": 2, "purpose": 0.65},
    )

    assert response.status_code == 200
    assert response.json()["request_id"] == "preview-1"
    mock_generate.assert_awaited_once()
    assert mock_generate.await_args.args[1:3] == ("did:plc:test-user", "your-feed")
    patch_doc = mock_generate.await_args.args[3]
    assert patch_doc.freshness == 2
    assert patch_doc.purpose == 0.65


@pytest.mark.parametrize(
    "source_weights",
    [
        {"following": 1, "network_likes": 0, "authors_topics": 0, "popular": 0},
        {"following": 0, "network_likes": 1, "authors_topics": 0, "popular": 0},
    ],
)
@patch("app.routers.feed_transparency.generate_feed_preview", new_callable=AsyncMock)
def test_create_feed_preview_preserves_100_percent_source_weights(
    mock_generate, client, source_weights
):
    mock_generate.return_value = _snapshot_doc(request_id="preview-100")

    response = client.post(
        "/api/feeds/your-feed/preview",
        json={"source_weights": source_weights},
    )

    assert response.status_code == 200
    patch_doc = mock_generate.await_args.args[3]
    assert patch_doc.source_weights is not None
    assert patch_doc.source_weights.model_dump() == source_weights


@patch("app.routers.feed_transparency.generate_feed_preview", new_callable=AsyncMock)
def test_create_feed_preview_accepts_empty_baseline_patch(mock_generate, client):
    mock_generate.return_value = _snapshot_doc(request_id="preview-baseline")

    response = client.post("/api/feeds/random/preview", json={})

    assert response.status_code == 200
    assert mock_generate.await_args.args[3].model_dump(exclude_none=True) == {}


@pytest.mark.parametrize(
    ("feed_name", "body", "expected_status"),
    [
        ("unknown", {}, 404),
        ("random", {"purpose": 0.65}, 422),
        ("your-feed", {"freshness": None}, 422),
    ],
)
def test_create_feed_preview_validates_feed_controls(client, feed_name, body, expected_status):
    response = client.post(f"/api/feeds/{feed_name}/preview", json=body)

    assert response.status_code == expected_status


@patch("app.routers.feed_transparency.hydrate_posts", new_callable=AsyncMock)
def test_get_feed_preview_reads_owned_preview_cache(mock_hydrate, client):
    snapshot = _snapshot_doc(request_id="preview-1")
    cache = MagicMock()
    cache.retrieve_document = AsyncMock(
        return_value=FeedCacheDocument(
            items=snapshot.items,
            items_meta=snapshot.items_meta,
            generator_diagnostics=snapshot.generator_diagnostics,
            user_did="did:plc:test-user",
            feed_name="your-feed",
            generated_at=snapshot.generated_at,
            api_release_sha=snapshot.api_release_sha,
            expires_at=datetime.now(UTC) + timedelta(minutes=10),
            mode="preview",
        )
    )
    app.state.feed_cache = cache
    uri = snapshot.items[0]
    mock_hydrate.return_value = {
        uri: {
            "author": {"handle": "alice.test", "display_name": "Alice", "avatar_url": None},
            "content": "Preview post",
            "created_at": datetime.now(UTC),
            "media": {},
            "engagement": {},
        }
    }

    response = client.get("/api/feeds/previews/preview-1")

    assert response.status_code == 200
    assert response.json()["items"][0]["content"] == "Preview post"


@patch("app.routers.feed_transparency.hydrate_posts", new_callable=AsyncMock)
def test_get_feed_preview_preserves_ranked_items_when_hydration_is_unavailable(
    mock_hydrate, client
):
    uris = [
        "at://did:plc:first/app.bsky.feed.post/one",
        "at://did:plc:second/app.bsky.feed.post/two",
    ]
    snapshot = _snapshot_doc(
        request_id="preview-partial",
        items=uris,
        items_meta=[
            PipelineItemMeta(
                at_uri=uri,
                rank=index,
                generators=[GeneratorMeta(name="followed_users", score=0.8)],
            )
            for index, uri in enumerate(uris, start=1)
        ],
        generator_diagnostics=[
            GeneratorDiagnostic(
                name="followed_users",
                weight=1.0,
                requested_count=100,
                returned_count=2,
                contributed_count=2,
            )
        ],
    )
    app.state.feed_cache = MagicMock(
        retrieve_document=AsyncMock(
            return_value=FeedCacheDocument(
                items=snapshot.items,
                items_meta=snapshot.items_meta,
                generator_diagnostics=snapshot.generator_diagnostics,
                user_did="did:plc:test-user",
                feed_name="your-feed",
                generated_at=snapshot.generated_at,
                expires_at=datetime.now(UTC) + timedelta(minutes=10),
                mode="preview",
            )
        )
    )
    mock_hydrate.return_value = {}

    response = client.get("/api/feeds/previews/preview-partial")

    assert response.status_code == 200
    data = response.json()
    assert [item["at_uri"] for item in data["items"]] == uris
    assert all(item["is_partial"] for item in data["items"])
    assert data["items"][0]["post_url"].endswith("/profile/did:plc:first/post/one")
    assert data["stored_item_count"] == 2
    assert data["displayed_item_count"] == 2
    assert data["unavailable_count"] == 2
    assert data["partial_item_count"] == 2
    assert data["generator_diagnostics"][0]["name"] == "followed_users"


def test_get_feed_preview_hides_wrong_owner(client):
    cache = MagicMock()
    cache.retrieve_document = AsyncMock(
        return_value=FeedCacheDocument(
            items=[],
            user_did="did:plc:someone-else",
            feed_name="your-feed",
            generated_at=datetime.now(UTC),
            expires_at=datetime.now(UTC) + timedelta(minutes=10),
            mode="preview",
        )
    )
    app.state.feed_cache = cache

    response = client.get("/api/feeds/previews/private-preview")

    assert response.status_code == 404


def test_get_feed_preview_hides_served_cache_entries(client):
    cache = MagicMock()
    cache.retrieve_document = AsyncMock(
        return_value=FeedCacheDocument(
            items=[],
            user_did="did:plc:test-user",
            feed_name="your-feed",
            generated_at=datetime.now(UTC),
            expires_at=datetime.now(UTC) + timedelta(minutes=10),
        )
    )
    app.state.feed_cache = cache

    response = client.get("/api/feeds/previews/served-request")

    assert response.status_code == 404


@patch("app.routers.feed_transparency.accept_feed_preview", new_callable=AsyncMock)
@pytest.mark.parametrize("cache_mode", ["preview", "accepted"])
def test_accept_preview_persists_the_exact_visible_slate(mock_accept, client, cache_mode):
    snapshot = _snapshot_doc(request_id="preview-accept")
    uri = snapshot.items[0]
    app.state.feed_cache = MagicMock(
        retrieve_document=AsyncMock(
            return_value=FeedCacheDocument(
                items=snapshot.items,
                items_meta=snapshot.items_meta,
                user_did="did:plc:test-user",
                feed_name="your-feed",
                generated_at=snapshot.generated_at,
                expires_at=datetime.now(UTC) + timedelta(minutes=5),
                mode=cache_mode,
                preference_patch=FeedPreferencesDocument(freshness=2),
            )
        )
    )
    mock_accept.return_value = (
        FeedPreferencesDocument(freshness=2, purpose=0.5),
        None,
    )

    response = client.post(
        "/api/feeds/your-feed/previews/preview-accept/accept",
        json={"preferences": {"freshness": 2}, "displayed_item_uris": [uri]},
    )

    assert response.status_code == 200
    assert response.json()["request_id"] == "preview-accept"
    assert response.json()["preferences"] == {"freshness": 2, "purpose": 0.5}
    assert response.json().get("accepted_until") is None
    mock_accept.assert_awaited_once()
    assert mock_accept.await_args.args[1:6] == (
        "did:plc:test-user",
        "your-feed",
        "preview-accept",
        FeedPreferencesDocument(freshness=2),
        [uri],
    )


@patch("app.routers.feed_transparency.accept_feed_preview", new_callable=AsyncMock)
def test_accept_preview_can_stage_a_genuinely_empty_visible_slate(mock_accept, client):
    snapshot = _snapshot_doc(request_id="preview-changed")
    app.state.feed_cache = MagicMock(
        retrieve_document=AsyncMock(
            return_value=FeedCacheDocument(
                items=snapshot.items,
                items_meta=snapshot.items_meta,
                user_did="did:plc:test-user",
                feed_name="your-feed",
                generated_at=snapshot.generated_at,
                expires_at=datetime.now(UTC) + timedelta(minutes=5),
                mode="preview",
                preference_patch=FeedPreferencesDocument(freshness=2),
            )
        )
    )
    mock_accept.return_value = (FeedPreferencesDocument(freshness=2, purpose=0.5), None)

    response = client.post(
        "/api/feeds/your-feed/previews/preview-changed/accept",
        json={"preferences": {"freshness": 2}, "displayed_item_uris": []},
    )

    assert response.status_code == 200
    assert mock_accept.await_args.args[5] == []


@patch(
    "app.routers.feed_transparency.accept_feed_preview",
    new_callable=AsyncMock,
    side_effect=StaleFeedPreviewError,
)
def test_accept_preview_maps_stale_preferences_to_conflict(mock_accept, client):
    snapshot = _snapshot_doc(request_id="preview-stale")
    app.state.feed_cache = MagicMock(
        retrieve_document=AsyncMock(
            return_value=FeedCacheDocument(
                items=snapshot.items,
                items_meta=snapshot.items_meta,
                user_did="did:plc:test-user",
                feed_name="your-feed",
                generated_at=snapshot.generated_at,
                expires_at=datetime.now(UTC) + timedelta(minutes=5),
                mode="preview",
                preference_patch=FeedPreferencesDocument(freshness=2),
            )
        )
    )

    response = client.post(
        "/api/feeds/your-feed/previews/preview-stale/accept",
        json={"preferences": {"freshness": 2}, "displayed_item_uris": snapshot.items},
    )

    assert response.status_code == 409
    assert "Settings changed" in response.json()["detail"]
    mock_accept.assert_awaited_once()


def test_saved_preview_acceptance_mysky_and_waist_share_one_organic_slate(client, monkeypatch):
    """Exercise the public lifecycle without devctl's synthetic feed path."""
    from ..documents import SourceWeightsDocument, UserDocument
    from .xrpc import _clear_initial_request_cache

    request_id = "lifecycle-preview-request"
    organic_uris = [
        "at://did:plc:author/app.bsky.feed.post/one",
        "at://did:plc:author/app.bsky.feed.post/two",
    ]
    preferences = FeedPreferencesDocument(
        source_weights=SourceWeightsDocument(
            following=1.0,
            network_likes=0.0,
            authors_topics=0.0,
            popular=0.0,
        ),
        freshness=3,
        purpose=0.65,
    )
    generated_at = datetime.now(UTC)
    preview_snapshot = FeedSnapshotDocument(
        request_id=request_id,
        items=organic_uris,
        feed_name="your-feed",
        generated_at=generated_at,
        expires_at=generated_at + timedelta(minutes=10),
        items_meta=[
            PipelineItemMeta(
                at_uri=uri,
                generators=[GeneratorMeta(name="followed_users", score=1.0)],
            )
            for uri in organic_uris
        ],
        generator_diagnostics=[
            GeneratorDiagnostic(
                name="followed_users",
                weight=1.0,
                requested_count=30,
                returned_count=2,
                contributed_count=2,
            )
        ],
    )
    cache_docs: dict[str, FeedCacheDocument] = {}
    waist_snapshots: dict[str, FeedSnapshotDocument] = {}
    cache = MagicMock()
    cache.retrieve_document = AsyncMock(side_effect=lambda key: cache_docs.get(key))

    async def generate_preview(_request, user_did, feed_name, patch_doc):
        assert patch_doc == preferences
        cache_docs[request_id] = FeedCacheDocument(
            items=organic_uris,
            items_meta=preview_snapshot.items_meta,
            generator_diagnostics=preview_snapshot.generator_diagnostics,
            user_did=user_did,
            feed_name=feed_name,
            generated_at=generated_at,
            expires_at=generated_at + timedelta(minutes=10),
            mode="preview",
            preference_patch=patch_doc,
            effective_preferences=preferences,
            preference_fingerprint="lifecycle-fingerprint",
        )
        return preview_snapshot

    async def accept_preview(_db, _did, _feed, accepted_id, _patch, displayed, **_kwargs):
        cached = cache_docs[accepted_id]
        cache_docs[accepted_id] = cached.model_copy(update={"items": displayed, "mode": "accepted"})
        return preferences, None

    async def write_snapshot(_db, _did, written_id, snapshot, **_kwargs):
        waist_snapshots[written_id] = snapshot

    async def hydrate(_db, uris):
        return {
            uri: {
                "author": {
                    "handle": "author.test",
                    "display_name": "Author",
                    "avatar_url": None,
                },
                "content": uri,
                "created_at": generated_at,
                "media": {},
                "engagement": {},
            }
            for uri in uris
        }

    app.state.feed_cache = cache
    monkeypatch.setenv("GE_FEED_CONTEXT_SECRET", "lifecycle-secret")
    _clear_initial_request_cache()
    user = UserDocument(
        user_did="did:plc:test-user",
        feed_preferences={"your-feed": preferences},
    )

    with (
        patch(
            "app.routers.feed_transparency.patch_user_feed_preferences",
            new_callable=AsyncMock,
            return_value=preferences,
        ),
        patch(
            "app.routers.feed_transparency.delete_most_recent_seen_bucket",
            new_callable=AsyncMock,
        ),
        patch(
            "app.routers.feed_transparency.generate_feed_preview",
            new_callable=AsyncMock,
            side_effect=generate_preview,
        ),
        patch(
            "app.routers.feed_transparency.accept_feed_preview",
            new_callable=AsyncMock,
            side_effect=accept_preview,
        ),
        patch(
            "app.routers.feed_transparency.hydrate_posts",
            new_callable=AsyncMock,
            side_effect=hydrate,
        ),
        patch(
            "app.routers.xrpc.verify_auth_header",
            new_callable=AsyncMock,
            return_value="did:plc:test-user",
        ),
        patch("app.routers.xrpc.get_user", new_callable=AsyncMock, return_value=user),
        patch(
            "app.routers.xrpc.claim_accepted_feed_slate",
            new_callable=AsyncMock,
            return_value=request_id,
        ),
        patch(
            "app.routers.xrpc._write_feed_snapshot_background",
            new_callable=AsyncMock,
            side_effect=write_snapshot,
        ),
        patch("app.routers.xrpc.upsert_user", new_callable=AsyncMock),
        patch("app.routers.xrpc.upsert_feed_activity", new_callable=AsyncMock),
        patch("app.routers.xrpc.get_posthog_client", return_value=None),
        patch(
            "app.routers.feed_transparency.get_recent_feed_snapshots",
            new_callable=AsyncMock,
            side_effect=lambda *_args, **_kwargs: list(waist_snapshots.values()),
        ),
        patch(
            "app.routers.feed_transparency.get_feed_snapshot",
            new_callable=AsyncMock,
            side_effect=lambda _db, _did, rid: waist_snapshots.get(rid),
        ),
    ):
        saved = client.patch(
            "/api/feeds/preferences/your-feed",
            json={
                "source_weights": {
                    "following": 1.0,
                    "network_likes": 0.0,
                    "authors_topics": 0.0,
                    "popular": 0.0,
                },
                "freshness": 3,
                "purpose": 0.65,
            },
        )
        created = client.post(
            "/api/feeds/your-feed/preview",
            json={
                "source_weights": {
                    "following": 1.0,
                    "network_likes": 0.0,
                    "authors_topics": 0.0,
                    "popular": 0.0,
                },
                "freshness": 3,
                "purpose": 0.65,
            },
        )
        hydrated_preview = client.get(f"/api/feeds/previews/{request_id}")
        accepted = client.post(
            f"/api/feeds/your-feed/previews/{request_id}/accept",
            json={
                "preferences": {
                    "source_weights": {
                        "following": 1.0,
                        "network_likes": 0.0,
                        "authors_topics": 0.0,
                        "popular": 0.0,
                    },
                    "freshness": 3,
                    "purpose": 0.65,
                },
                "displayed_item_uris": organic_uris,
            },
        )
        mysky = client.get(
            "/xrpc/app.bsky.feed.getFeedSkeleton",
            params={
                "feed": "at://did:web:api-stage.greenearth.social/app.bsky.feed.generator/your-feed",
                "limit": 10,
            },
        )
        waist_list = client.get("/api/feeds")
        waist_detail = client.get(f"/api/feeds/{request_id}")

    assert saved.status_code == 200
    assert created.json()["request_id"] == request_id
    assert [item["at_uri"] for item in hydrated_preview.json()["items"]] == organic_uris
    assert accepted.json()["request_id"] == request_id
    served = [item["post"] for item in mysky.json()["feed"]]
    assert served[1:] == organic_uris  # MySky's configured pin remains presentation-only.
    assert waist_list.json()["feeds"][0]["request_id"] == request_id
    assert waist_detail.json()["request_id"] == request_id
    assert [item["at_uri"] for item in waist_detail.json()["items"]] == organic_uris


@patch("app.routers.feed_transparency.hydrate_posts")
@patch("app.routers.feed_transparency.get_feed_snapshot")
def test_get_feed_detail_returns_merged_data(mock_get_snapshot, mock_hydrate, client):
    uri = "at://did:plc:author/app.bsky.feed.post/post1"
    doc = _snapshot_doc(api_release_sha="api-sha-detail")
    mock_get_snapshot.return_value = doc
    mock_hydrate.return_value = {
        uri: {
            "author": {
                "handle": "alice.bsky.social",
                "display_name": "Alice Chen",
                "avatar_url": "https://cdn.bsky.app/avatar.jpg",
            },
            "content": "Hello world",
            "created_at": datetime(2026, 7, 12, 10, 0, tzinfo=UTC),
            "media": {
                "image_urls": [],
                "video_url": None,
                "link_card_url": None,
                "link_card_title": None,
                "link_card_description": None,
                "labels": ["1 image"],
            },
            "engagement": {"reply_count": 3, "repost_count": 12, "like_count": 47},
        }
    }

    response = client.get("/api/feeds/req-abc")
    assert response.status_code == 200
    data = response.json()
    assert data["request_id"] == "req-abc"
    assert data["api_release_sha"] == "api-sha-detail"
    assert len(data["items"]) == 1

    item = data["items"][0]
    assert item["at_uri"] == uri
    assert item["rank"] == 1
    assert item["rank_score"] == 0.92
    assert item["author"]["handle"] == "alice.bsky.social"
    assert item["author"]["display_name"] == "Alice Chen"
    assert item["content"] == "Hello world"
    assert item["post_url"] == "https://bsky.app/profile/alice.bsky.social/post/post1"
    assert item["engagement"]["reply_count"] == 3
    assert len(item["generators"]) == 1
    assert item["generators"][0]["name"] == "two_tower"
    assert item["generators"][0]["score"] == 0.85
    assert len(item["model_scores"]) == 1
    assert item["model_scores"][0]["name"] == "two_tower"
    assert item["model_scores"][0]["score"] == 0.92
    assert item["diversification"]["relevance"] == 0.95


@patch("app.routers.feed_transparency.get_feed_snapshot")
def test_get_feed_detail_not_found(mock_get_snapshot, client):
    mock_get_snapshot.return_value = None

    response = client.get("/api/feeds/nonexistent")
    assert response.status_code == 404


@patch("app.routers.feed_transparency.hydrate_posts")
@patch("app.routers.feed_transparency.get_feed_snapshot")
def test_get_feed_detail_uses_snake_case_keys(mock_get_snapshot, mock_hydrate, client):
    uri = "at://did:plc:author/app.bsky.feed.post/post1"
    doc = _snapshot_doc()
    mock_get_snapshot.return_value = doc
    mock_hydrate.return_value = {
        uri: {
            "author": {"handle": "alice.bsky.social", "display_name": None, "avatar_url": None},
            "content": "",
            "created_at": None,
            "media": {
                "image_urls": [],
                "video_url": None,
                "link_card_url": None,
                "link_card_title": None,
                "link_card_description": None,
                "labels": [],
            },
            "engagement": {"reply_count": 0, "repost_count": 0, "like_count": 0},
        }
    }

    response = client.get("/api/feeds/req-abc")
    data = response.json()

    assert "request_id" in data
    assert "requestId" not in data
    item = data["items"][0]
    assert "at_uri" in item
    assert "rank_score" in item
    assert "after_rank_position" in item
    assert "model_scores" in item
    assert "post_url" in item
    assert "atUri" not in item


@patch("app.routers.feed_transparency.hydrate_posts")
@patch("app.routers.feed_transparency.get_feed_snapshot")
def test_get_feed_detail_diversification_null_when_absent(mock_get_snapshot, mock_hydrate, client):
    uri = "at://did:plc:author/app.bsky.feed.post/post1"
    doc = _snapshot_doc(
        diversify=False,
        items_meta=[
            PipelineItemMeta(
                at_uri=uri,
                rank=1,
                rank_score=None,
                after_rank_position=1,
                generators=[GeneratorMeta(name="two_tower", score=0.85)],
                model_scores=[],
                diversification=None,
            )
        ],
    )
    mock_get_snapshot.return_value = doc
    mock_hydrate.return_value = {
        uri: {
            "author": {"handle": "x", "display_name": None, "avatar_url": None},
            "content": "",
            "created_at": None,
            "media": {
                "image_urls": [],
                "video_url": None,
                "link_card_url": None,
                "link_card_title": None,
                "link_card_description": None,
                "labels": [],
            },
            "engagement": {"reply_count": 0, "repost_count": 0, "like_count": 0},
        }
    }

    response = client.get("/api/feeds/req-abc")
    data = response.json()
    assert data["items"][0]["diversification"] is None


@patch("app.routers.feed_transparency.hydrate_posts")
@patch("app.routers.feed_transparency.get_feed_snapshot")
def test_get_feed_detail_multiple_items(mock_get_snapshot, mock_hydrate, client):
    uri1 = "at://did:plc:a/app.bsky.feed.post/p1"
    uri2 = "at://did:plc:b/app.bsky.feed.post/p2"

    doc = _snapshot_doc(
        items_meta=[
            PipelineItemMeta(
                at_uri=uri1,
                rank=1,
                rank_score=0.92,
                after_rank_position=1,
                generators=[GeneratorMeta(name="two_tower", score=0.85)],
                model_scores=[ModelScoreMeta(name="two_tower", weight=1.0, score=0.92)],
            ),
            PipelineItemMeta(
                at_uri=uri2,
                rank=2,
                rank_score=0.88,
                after_rank_position=2,
                generators=[GeneratorMeta(name="two_tower", score=0.80)],
                model_scores=[ModelScoreMeta(name="two_tower", weight=1.0, score=0.88)],
            ),
        ],
        items=[uri1, uri2],
    )
    mock_get_snapshot.return_value = doc
    mock_hydrate.return_value = {
        uri1: {
            "author": {"handle": "alice.bsky.social", "display_name": None, "avatar_url": None},
            "content": "first",
            "created_at": None,
            "media": {
                "image_urls": [],
                "video_url": None,
                "link_card_url": None,
                "link_card_title": None,
                "link_card_description": None,
                "labels": [],
            },
            "engagement": {"reply_count": 0, "repost_count": 0, "like_count": 0},
        },
        uri2: {
            "author": {"handle": "bob.bsky.social", "display_name": None, "avatar_url": None},
            "content": "second",
            "created_at": None,
            "media": {
                "image_urls": [],
                "video_url": None,
                "link_card_url": None,
                "link_card_title": None,
                "link_card_description": None,
                "labels": [],
            },
            "engagement": {"reply_count": 0, "repost_count": 0, "like_count": 0},
        },
    }

    response = client.get("/api/feeds/req-abc")
    data = response.json()
    assert len(data["items"]) == 2
    assert data["items"][0]["at_uri"] == uri1
    assert data["items"][0]["content"] == "first"
    assert data["items"][1]["at_uri"] == uri2
    assert data["items"][1]["content"] == "second"


# ---------------------------------------------------------------------------
# GET /api/feeds/preferences and PATCH /api/feeds/preferences/{feed_name}
# ---------------------------------------------------------------------------


@patch("app.routers.feed_transparency.get_user")
def test_get_preferences_returns_default_for_new_user(mock_get_user, client):
    from ..documents import UserDocument

    mock_get_user.return_value = UserDocument(
        user_did="did:plc:test-user",
        username="test.bsky.social",
    )

    response = client.get("/api/feeds/preferences")
    assert response.status_code == 200
    assert response.json() == {
        "feeds": {
            "random": {"freshness": 5},
            "your-feed": {
                "source_weights": {
                    "following": 0.3,
                    "network_likes": 0.2,
                    "authors_topics": 0.25,
                    "popular": 0.25,
                },
                "freshness": 5,
                "purpose": 0.5,
            },
            "best-of-friends": {"freshness": 5, "purpose": 0.5},
        }
    }


@patch("app.routers.feed_transparency.get_user")
def test_get_preferences_returns_stored_value(mock_get_user, client):
    from ..documents import UserDocument

    mock_get_user.return_value = UserDocument(
        user_did="did:plc:test-user",
        username="test.bsky.social",
        social_radius=0,
        freshness=3,
        politics=1.25,
        purpose=0.65,
    )

    response = client.get("/api/feeds/preferences")
    assert response.status_code == 200
    assert response.json()["feeds"] == {
        "random": {"freshness": 3},
        "your-feed": {
            "source_weights": {
                "following": 1.0,
                "network_likes": 0.0,
                "authors_topics": 0.0,
                "popular": 0.0,
            },
            "freshness": 3,
            "purpose": 0.65,
        },
        "best-of-friends": {"freshness": 3, "purpose": 0.65},
    }


@patch("app.routers.feed_transparency.delete_most_recent_seen_bucket")
@patch("app.routers.feed_transparency.patch_user_feed_preferences")
def test_patch_preferences_updates_only_selected_feed(mock_patch_prefs, mock_delete_seen, client):
    from ..documents import FeedPreferencesDocument

    mock_patch_prefs.return_value = FeedPreferencesDocument(
        freshness=4,
        purpose=0.65,
    )
    response = client.patch(
        "/api/feeds/preferences/best-of-friends",
        json={"freshness": 4},
    )
    assert response.status_code == 200
    assert response.json() == {"freshness": 4, "purpose": 0.65}
    args = mock_patch_prefs.await_args.args
    assert args[1:3] == ("did:plc:test-user", "best-of-friends")
    assert args[3].model_dump(exclude_none=True) == {"freshness": 4}
    mock_delete_seen.assert_awaited_once()


@patch("app.routers.feed_transparency.delete_most_recent_seen_bucket")
@patch("app.routers.feed_transparency.patch_user_feed_preferences")
def test_patch_preferences_succeeds_when_seen_cleanup_fails(
    mock_patch_prefs, mock_delete_seen, client
):
    mock_patch_prefs.return_value = FeedPreferencesDocument(freshness=4, purpose=0.65)
    mock_delete_seen.side_effect = RuntimeError("cleanup unavailable")

    response = client.patch(
        "/api/feeds/preferences/best-of-friends",
        json={"freshness": 4},
    )

    assert response.status_code == 200
    assert response.json() == {"freshness": 4, "purpose": 0.65}
    mock_delete_seen.assert_awaited_once()


@patch("app.routers.feed_transparency.patch_user_feed_preferences")
def test_patch_preferences_rejects_out_of_range(mock_patch_prefs, client):
    response = client.patch(
        "/api/feeds/preferences/random",
        json={"freshness": 10},
    )
    assert response.status_code == 422
    mock_patch_prefs.assert_not_awaited()


@pytest.mark.parametrize(
    "weight_values",
    [
        {
            "following": 0.3,
            "network_likes": 0.2,
            "authors_topics": 0.25,
            "popular": 0.25,
        },
        {"following": 1.0, "network_likes": 0.0, "authors_topics": 0.0, "popular": 0.0},
        {"following": 0.0, "network_likes": 1.0, "authors_topics": 0.0, "popular": 0.0},
        {"following": 0.0, "network_likes": 0.0, "authors_topics": 1.0, "popular": 0.0},
        {"following": 0.0, "network_likes": 0.0, "authors_topics": 0.0, "popular": 1.0},
    ],
)
@patch("app.routers.feed_transparency.delete_most_recent_seen_bucket")
@patch("app.routers.feed_transparency.patch_user_feed_preferences")
def test_patch_preferences_accepts_atomic_source_weights(
    mock_patch_prefs, mock_delete_seen, weight_values, client
):
    from ..documents import FeedPreferencesDocument, SourceWeightsDocument

    weights = SourceWeightsDocument(**weight_values)
    mock_patch_prefs.return_value = FeedPreferencesDocument(source_weights=weights)

    response = client.patch(
        "/api/feeds/preferences/your-feed",
        json={"source_weights": weights.model_dump()},
    )

    assert response.status_code == 200
    assert response.json() == {"source_weights": weights.model_dump()}
    assert mock_patch_prefs.await_args.args[3].source_weights == weights


@pytest.mark.parametrize(
    "weights",
    [
        {"following": 0.5, "authors_topics": 0.2},
        {"following": 0.5, "authors_topics": 0.2, "popular": 0.2},
        {"following": -0.1, "authors_topics": 0.4, "popular": 0.7},
        {"following": 0.0, "authors_topics": 1.1, "popular": -0.1},
    ],
)
@patch("app.routers.feed_transparency.patch_user_feed_preferences")
def test_patch_preferences_rejects_invalid_source_weights(mock_patch_prefs, weights, client):
    response = client.patch(
        "/api/feeds/preferences/your-feed",
        json={"source_weights": weights},
    )
    assert response.status_code == 422
    mock_patch_prefs.assert_not_awaited()


@patch("app.routers.feed_transparency.patch_user_feed_preferences")
def test_patch_preferences_rejects_unsupported_control(mock_patch_prefs, client):
    response = client.patch(
        "/api/feeds/preferences/random",
        json={"purpose": 0.8},
    )
    assert response.status_code == 422
    mock_patch_prefs.assert_not_awaited()


@pytest.mark.parametrize("body", [{}, {"freshness": None}, {"socialRadius": 3}])
@patch("app.routers.feed_transparency.patch_user_feed_preferences")
def test_patch_preferences_rejects_invalid_body(mock_patch_prefs, body, client):
    response = client.patch(
        "/api/feeds/preferences/your-feed",
        json=body,
    )
    assert response.status_code == 422
    mock_patch_prefs.assert_not_awaited()


def test_patch_preferences_rejects_unknown_or_internal_feed(client):
    assert client.patch("/api/feeds/preferences/nope", json={"freshness": 2}).status_code == 404
    assert (
        client.patch("/api/feeds/preferences/cold-start", json={"freshness": 2}).status_code == 404
    )


# ---------------------------------------------------------------------------
# _at_uri_to_bsky_url
# ---------------------------------------------------------------------------


def test_at_uri_to_bsky_url():
    from .feed_transparency import _at_uri_to_bsky_url

    assert (
        _at_uri_to_bsky_url("at://did:plc:abc/app.bsky.feed.post/post1")
        == "https://bsky.app/profile/did:plc:abc/post/post1"
    )
    assert (
        _at_uri_to_bsky_url("at://did:plc:abc/app.bsky.feed.post/post1", "alice.bsky.social")
        == "https://bsky.app/profile/alice.bsky.social/post/post1"
    )
    assert _at_uri_to_bsky_url("at://did:plc:abc/app.bsky.feed.like/xyz") is None
    assert _at_uri_to_bsky_url("not-a-uri") is None


# ---------------------------------------------------------------------------
# GET /api/feeds/{request_id} — deduplication across newer snapshots
# ---------------------------------------------------------------------------


def _hydrated(uri: str, handle: str = "alice.bsky.social") -> dict:
    return {
        uri: {
            "author": {"handle": handle, "display_name": None, "avatar_url": None},
            "content": "",
            "created_at": None,
            "media": {
                "image_urls": [],
                "video_url": None,
                "link_card_url": None,
                "link_card_title": None,
                "link_card_description": None,
                "labels": [],
            },
            "engagement": {"reply_count": 0, "repost_count": 0, "like_count": 0},
        }
    }


@patch("app.routers.feed_transparency.hydrate_posts")
@patch("app.routers.feed_transparency.get_feed_snapshot")
def test_get_feed_detail_preserves_items_seen_in_newer_snapshots(
    mock_get_snapshot, mock_hydrate, client
):
    uri1 = "at://did:plc:a/app.bsky.feed.post/p1"
    uri2 = "at://did:plc:b/app.bsky.feed.post/p2"
    doc = _snapshot_doc(
        items_meta=[
            PipelineItemMeta(
                at_uri=uri1,
                rank=1,
                rank_score=0.92,
                after_rank_position=1,
                generators=[GeneratorMeta(name="two_tower", score=0.85)],
                model_scores=[ModelScoreMeta(name="heavy_ranker", weight=1.0, score=0.92)],
            ),
            PipelineItemMeta(
                at_uri=uri2,
                rank=2,
                rank_score=0.88,
                after_rank_position=2,
                generators=[GeneratorMeta(name="popularity", score=0.80)],
                model_scores=[ModelScoreMeta(name="heavy_ranker", weight=1.0, score=0.88)],
            ),
        ],
        items=[uri1, uri2],
    )
    mock_get_snapshot.return_value = doc
    mock_hydrate.return_value = {**_hydrated(uri2, "bob.bsky.social")}

    response = client.get("/api/feeds/req-abc")
    data = response.json()

    assert response.status_code == 200
    assert len(data["items"]) == 1
    assert [item["at_uri"] for item in data["items"]] == [uri2]


@patch("app.routers.feed_transparency.hydrate_posts")
@patch("app.routers.feed_transparency.get_feed_snapshot")
def test_get_feed_detail_hides_unavailable_posts_but_preserves_valid_order(
    mock_get_snapshot, mock_hydrate, client
):
    unavailable = "at://did:plc:a/app.bsky.feed.post/deleted"
    first = "at://did:plc:b/app.bsky.feed.post/p1"
    second = "at://did:plc:c/app.bsky.feed.post/p2"
    mock_get_snapshot.return_value = _snapshot_doc(
        items=[unavailable, first, second],
        items_meta=[
            PipelineItemMeta(at_uri=unavailable),
            PipelineItemMeta(at_uri=first),
            PipelineItemMeta(at_uri=second),
        ],
    )
    mock_hydrate.return_value = {
        unavailable: {
            "author": {"handle": None, "display_name": None, "avatar_url": None},
            "content": None,
            "created_at": None,
            "media": {"image_urls": [], "labels": []},
            "engagement": {"reply_count": 0, "repost_count": 0, "like_count": 0},
        },
        **_hydrated(first, "first.bsky.social"),
        **_hydrated(second, "second.bsky.social"),
    }

    response = client.get("/api/feeds/req-abc")

    assert response.status_code == 200
    assert [item["at_uri"] for item in response.json()["items"]] == [first, second]
    assert response.json()["stored_item_count"] == 3
    assert response.json()["displayed_item_count"] == 2
    assert response.json()["publicly_filtered_count"] == 0
    assert response.json()["unavailable_count"] == 1


@patch("app.routers.feed_transparency.hydrate_posts")
@patch("app.routers.feed_transparency.get_feed_snapshot")
def test_get_feed_detail_filters_public_post_and_author_labels(
    mock_get_snapshot, mock_hydrate, client
):
    safe = "at://did:plc:a/app.bsky.feed.post/safe"
    labeled_post = "at://did:plc:b/app.bsky.feed.post/labeled"
    labeled_author = "at://did:plc:c/app.bsky.feed.post/author-labeled"
    mock_get_snapshot.return_value = _snapshot_doc(
        items=[safe, labeled_post, labeled_author],
        items_meta=[PipelineItemMeta(at_uri=uri) for uri in [safe, labeled_post, labeled_author]],
    )
    hydrated = {
        **_hydrated(safe, "safe.bsky.social"),
        **_hydrated(labeled_post, "post.bsky.social"),
        **_hydrated(labeled_author, "author.bsky.social"),
    }
    hydrated[safe]["moderation"] = {"post_labels": [], "author_labels": []}
    hydrated[labeled_post]["moderation"] = {"post_labels": ["graphic-media"], "author_labels": []}
    hydrated[labeled_author]["moderation"] = {"post_labels": [], "author_labels": ["porn"]}
    mock_hydrate.return_value = hydrated

    response = client.get("/api/feeds/req-abc")
    data = response.json()

    assert response.status_code == 200
    assert [item["at_uri"] for item in data["items"]] == [safe]
    assert data["stored_item_count"] == 3
    assert data["displayed_item_count"] == 1
    assert data["publicly_filtered_count"] == 2
    assert data["unavailable_count"] == 0


@patch("app.routers.feed_transparency.hydrate_posts")
@patch("app.routers.feed_transparency.get_feed_snapshot")
def test_get_feed_detail_returns_all_when_no_newer_snapshots(
    mock_get_snapshot, mock_hydrate, client
):
    uri1 = "at://did:plc:a/app.bsky.feed.post/p1"
    uri2 = "at://did:plc:b/app.bsky.feed.post/p2"
    doc = _snapshot_doc(
        items_meta=[
            PipelineItemMeta(
                at_uri=uri1,
                rank=1,
                rank_score=0.92,
                after_rank_position=1,
                generators=[GeneratorMeta(name="two_tower", score=0.85)],
                model_scores=[ModelScoreMeta(name="heavy_ranker", weight=1.0, score=0.92)],
            ),
            PipelineItemMeta(
                at_uri=uri2,
                rank=2,
                rank_score=0.88,
                after_rank_position=2,
                generators=[GeneratorMeta(name="popularity", score=0.80)],
                model_scores=[ModelScoreMeta(name="heavy_ranker", weight=1.0, score=0.88)],
            ),
        ],
        items=[uri1, uri2],
    )
    mock_get_snapshot.return_value = doc
    mock_hydrate.return_value = {**_hydrated(uri1), **_hydrated(uri2, "bob.bsky.social")}

    response = client.get("/api/feeds/req-abc")
    data = response.json()

    assert response.status_code == 200
    assert len(data["items"]) == 2


# ---------------------------------------------------------------------------
# GET /api/feeds/{request_id} — diverse pipeline metadata
# ---------------------------------------------------------------------------


@patch("app.routers.feed_transparency.hydrate_posts")
@patch("app.routers.feed_transparency.get_feed_snapshot")
def test_get_feed_detail_diverse_pipeline_metadata(mock_get_snapshot, mock_hydrate, client):
    uri = "at://did:plc:author/app.bsky.feed.post/post1"
    doc = _snapshot_doc(
        items_meta=[
            PipelineItemMeta(
                at_uri=uri,
                rank=1,
                rank_score=0.92,
                after_rank_position=1,
                generators=[
                    GeneratorMeta(name="two_tower", score=0.85),
                    GeneratorMeta(name="followed_users", score=0.70),
                    GeneratorMeta(name="popularity", score=0.60),
                ],
                model_scores=[
                    ModelScoreMeta(name="heavy_ranker", weight=1.0, score=0.92),
                    ModelScoreMeta(name="perspective", weight=1.0, score=0.425),
                ],
            )
        ],
        generator_legend=[
            GeneratorMeta(name="two_tower", weight=0.35),
            GeneratorMeta(name="followed_users", weight=0.35),
            GeneratorMeta(name="popularity", weight=0.3),
        ],
        ranker_model="heavy_ranker, perspective",
    )
    mock_get_snapshot.return_value = doc
    mock_hydrate.return_value = {
        uri: {
            "author": {"handle": "alice.bsky.social", "display_name": "Alice", "avatar_url": None},
            "content": "hello",
            "created_at": None,
            "media": {
                "image_urls": [],
                "video_url": None,
                "link_card_url": None,
                "link_card_title": None,
                "link_card_description": None,
                "labels": [],
            },
            "engagement": {"reply_count": 0, "repost_count": 0, "like_count": 0},
        }
    }

    response = client.get("/api/feeds/req-abc")
    data = response.json()

    assert response.status_code == 200
    item = data["items"][0]

    gen_names = [g["name"] for g in item["generators"]]
    assert gen_names == ["two_tower", "followed_users", "popularity"]

    model_names = [m["name"] for m in item["model_scores"]]
    assert model_names == ["heavy_ranker", "perspective"]
    assert item["model_scores"][0]["weight"] == 1.0
    assert item["model_scores"][0]["score"] == 0.92
    assert item["model_scores"][1]["weight"] == 1.0
    assert item["model_scores"][1]["score"] == 0.425
