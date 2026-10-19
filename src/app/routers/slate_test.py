"""Tests for POST /slate/generate — the whole pipeline in one call."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from ..documents import FeedCacheDocument, StoredSlateDocument
from ..lib.candidates.base import CandidateResult
from ..lib.embeddings import encode_float32_b64
from ..lib.feed_cache import InMemoryFeedCache
from ..lib.feed_context import decode_feed_context
from ..lib.metrics import MetricCollector, set_metric_collector
from ..main import app
from ..models import CandidatePost, FeedCursor
from ..security import verify_api_key

USER_DID = "did:plc:slateuser"
FEED_NAME = "partner-feed"
TEST_EMBEDDING = encode_float32_b64([1.0, 0.0, 0.0])


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("GE_FEED_CONTEXT_SECRET", "test-feed-context-secret")


@pytest.fixture(autouse=True)
def fake_app_state():
    app.state.es = AsyncMock()
    app.state.firestore = AsyncMock()
    app.state.feed_cache = InMemoryFeedCache()
    yield
    for attr in ("es", "firestore", "feed_cache"):
        try:
            delattr(app.state, attr)
        except Exception:
            pass


@pytest.fixture(autouse=True)
def no_user_history():
    """No seen/discarded history for the user, and capture discard writes."""
    with (
        patch("app.routers.xrpc.get_recent_seen_uris", new_callable=AsyncMock, return_value=[]),
        patch(
            "app.routers.xrpc.get_recent_discarded_uris", new_callable=AsyncMock, return_value=[]
        ),
        # Nothing on this path needs a real ES round-trip for embeddings.
        patch("app.lib.slate_pipeline.hydrate_posts", new=AsyncMock(side_effect=lambda es, c: c)),
    ):
        yield


@pytest.fixture(autouse=True)
def stored_slates():
    """In-memory stand-in for the Firestore last-load and preview documents.

    The real helpers are transactional Firestore code tested in
    lib/firestore_test.py. Here they only need to round-trip, and a served
    preview claim materializes into the in-memory feed cache as the real one
    does into the feed_cache collection.
    """
    store: dict[tuple[str, str, str, str], StoredSlateDocument] = {}

    async def get(db, user_did, *, api_key_id, feed_name, kind):
        return store.get((user_did, api_key_id, feed_name, kind))

    async def save(db, user_did, *, api_key_id, feed_name, kind, stored):
        store[(user_did, api_key_id, feed_name, kind)] = stored

    async def delete(db, user_did, *, api_key_id, feed_name, kind):
        store.pop((user_did, api_key_id, feed_name, kind), None)

    async def claim(db, user_did, *, api_key_id, feed_name, settings_fingerprint, **_):
        key = (user_did, api_key_id, feed_name, "preview")
        preview = store.get(key)
        if preview is None:
            return None, "none"
        if preview.settings_fingerprint != settings_fingerprint:
            del store[key]
            return None, "settings_changed"
        if preview.claimed_at is not None:  # no grace window in the fake
            del store[key]
            return None, "consumed"
        store[key] = preview.model_copy(update={"claimed_at": datetime.now(UTC)})
        await app.state.feed_cache.store_document(
            preview.request_id,
            preview.slate.model_copy(update={"mode": "api", "served_from": "preview"}),
        )
        return preview.request_id, "served"

    with (
        patch("app.routers.slate.get_api_slate", side_effect=get),
        patch("app.routers.slate.save_api_slate", side_effect=save),
        patch("app.routers.slate.delete_api_slate", side_effect=delete),
        patch("app.routers.slate.claim_api_preview", side_effect=claim) as claim_mock,
    ):
        yield SimpleNamespace(store=store, claim=claim_mock)


@pytest.fixture
def discarded():
    """Capture _record_discarded without running it in the background."""
    recorded = AsyncMock()
    with (
        patch("app.routers.slate._record_discarded", new=recorded),
        patch("app.routers.slate._spawn_background", new=lambda coro: coro.close()),
    ):
        yield recorded


client = TestClient(app)


def _candidates(prefix: str, scores: list[float | None], *, embedded: bool = True):
    return [
        CandidatePost(
            at_uri=f"at://{prefix}/{i}",
            content=f"post {i}",
            minilm_l12_embedding=TEST_EMBEDDING if embedded else None,
            score=score,
            author_did=f"did:plc:author{i}",
        )
        for i, score in enumerate(scores)
    ]


def _fake_generators(**by_name: list[CandidatePost]):
    """Patch generator lookup (for validation and execution) with canned results."""
    gens = {}
    for name, candidates in by_name.items():
        gen = AsyncMock()
        gen.name = name
        gen.generate.return_value = CandidateResult(generator_name=name, candidates=candidates)
        gens[name] = gen
    lookup = lambda name: gens.get(name)  # noqa: E731
    return (
        patch("app.lib.candidates.generate.get_generator", side_effect=lookup),
        patch("app.routers.slate.get_generator", side_effect=lookup),
        gens,
    )


def _body(**overrides):
    body = {
        "feed_name": FEED_NAME,
        "user_did": USER_DID,
        "limit": 10,
        "num_candidates": 20,
        "generators": [{"name": "popularity", "weight": 1.0}],
        "diversify": False,
    }
    body.update(overrides)
    return body


def _post(body: dict):
    return client.post("/slate/generate", json=body, headers={"X-API-Key": "testkey"})


# ---------------------------------------------------------------------------
# Fresh generation
# ---------------------------------------------------------------------------


class TestGenerate:
    def test_unranked_slate_orders_by_generator_score_with_metadata_and_context(self):
        gen_patch, lookup_patch, _ = _fake_generators(
            popularity=_candidates("pop", [0.2, 0.9, 0.5])
        )
        with gen_patch, lookup_patch:
            resp = _post(_body())

        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert [item["at_uri"] for item in data["items"]] == [
            "at://pop/1", "at://pop/2", "at://pop/0"
        ]
        assert data["cursor"] is None
        assert data["feed_name"] == FEED_NAME
        assert data["degraded"] == []

        first = data["items"][0]
        assert first["rank"] == 1  # final position, even when unranked
        assert first["rank_score"] is None
        assert first["model_scores"] == []
        assert [g["name"] for g in first["generators"]] == ["popularity"]
        assert first["diversification"] is None

        payload = decode_feed_context(first["feed_context"])
        assert payload is not None
        assert (payload.did, payload.feed, payload.rid) == (USER_DID, FEED_NAME, data["request_id"])
        assert {item["feed_context"] for item in data["items"]} == {first["feed_context"]}

        (diag,) = data["generator_diagnostics"]
        assert diag["name"] == "popularity"
        assert diag["returned_count"] == 3
        assert diag["contributed_count"] == 3

    def test_external_candidates_take_their_weighted_share_from_our_index(self):
        gen_patch, lookup_patch, gens = _fake_generators(
            popularity=_candidates("pop", [0.4, 0.3, 0.2, 0.1])
        )
        # The external lookup finds two of the three supplied URIs.
        app.state.es.search = AsyncMock(
            return_value={
                "hits": {
                    "hits": [
                        {
                            "_score": 1.0,
                            "_source": {"at_uri": "at://ext/b", "author_did": "did:plc:b"},
                        },
                        {
                            "_score": 1.0,
                            "_source": {"at_uri": "at://ext/a", "author_did": "did:plc:a"},
                        },
                    ]
                }
            }
        )
        with gen_patch, lookup_patch:
            resp = _post(
                _body(
                    num_candidates=4,
                    generators=[
                        {"name": "external", "weight": 0.5},
                        {"name": "popularity", "weight": 0.5},
                    ],
                    external_candidates=[
                        {"at_uri": "at://ext/a", "score": 0.95},
                        {"at_uri": "at://ext/missing", "score": 0.9},
                        {"at_uri": "at://ext/b"},
                    ],
                )
            )

        assert resp.status_code == 200, resp.text
        data = resp.json()
        uris = [item["at_uri"] for item in data["items"]]
        # Caller's score wins the unranked ordering; a missing-score post sorts last.
        assert uris == ["at://ext/a", "at://pop/0", "at://pop/1", "at://ext/b"]
        # popularity got its half of the allocation.
        assert gens["popularity"].generate.await_args.kwargs["num_candidates"] == 2
        # ES was asked for every non-excluded supplied URI, in caller order.
        search_call = app.state.es.search.await_args
        assert search_call is not None
        assert search_call.kwargs["query"] == {
            "terms": {"at_uri": ["at://ext/a", "at://ext/missing", "at://ext/b"]}
        }
        by_name = {d["name"]: d for d in data["generator_diagnostics"]}
        assert by_name["external"]["requested_count"] == 2
        assert by_name["external"]["returned_count"] == 2

    def test_caller_exclusions_merge_with_user_history(self):
        gen_patch, lookup_patch, gens = _fake_generators(popularity=_candidates("pop", [0.5]))
        with (
            gen_patch,
            lookup_patch,
            patch(
                "app.routers.xrpc.get_recent_seen_uris",
                new_callable=AsyncMock,
                return_value=["at://seen/1"],
            ),
            patch(
                "app.routers.xrpc.get_recent_discarded_uris",
                new_callable=AsyncMock,
                return_value=["at://discarded/1", "at://caller/1"],
            ),
        ):
            resp = _post(_body(exclude_uris=["at://caller/1", "at://caller/2"]))

        assert resp.status_code == 200, resp.text
        assert gens["popularity"].generate.await_args.kwargs["exclude_uris"] == [
            "at://caller/1", "at://caller/2", "at://seen/1", "at://discarded/1"
        ]

    def test_ranked_slate_applies_cutoffs_and_records_discards(self, discarded):
        gen_patch, lookup_patch, _ = _fake_generators(
            popularity=_candidates("pop", [0.9, 0.8, 0.7, 0.2, 0.1, 0.05])
        )
        with gen_patch, lookup_patch:
            resp = _post(
                _body(
                    rankers=[{"name": "candidate_score", "weight": 1.0}],
                    min_rank_score=0.5,
                    max_render_share=0.5,
                )
            )

        assert resp.status_code == 200, resp.text
        data = resp.json()
        uris = [item["at_uri"] for item in data["items"]]
        # min_rank_score cuts three; max_render_share caps at floor(0.5 * 6) = 3.
        assert uris == ["at://pop/0", "at://pop/1", "at://pop/2"]
        assert [item["rank"] for item in data["items"]] == [1, 2, 3]
        assert data["items"][0]["rank_score"] == pytest.approx(0.9)
        assert [m["name"] for m in data["items"][0]["model_scores"]] == ["candidate_score"]
        discarded.assert_called_once()
        assert discarded.call_args.args[1:] == (USER_DID, ["at://pop/3", "at://pop/4", "at://pop/5"])

    def test_ranking_drops_candidates_without_embeddings(self):
        gen_patch, lookup_patch, _ = _fake_generators(
            popularity=[
                *_candidates("emb", [0.5]),
                *_candidates("noemb", [0.9], embedded=False),
            ]
        )
        with gen_patch, lookup_patch:
            resp = _post(_body(rankers=[{"name": "candidate_score", "weight": 1.0}]))

        assert resp.status_code == 200, resp.text
        assert [item["at_uri"] for item in resp.json()["items"]] == ["at://emb/0"]

    def test_diversify_records_mmr_metadata(self):
        gen_patch, lookup_patch, _ = _fake_generators(popularity=_candidates("pop", [0.9, 0.8]))
        with gen_patch, lookup_patch:
            resp = _post(_body(diversify=True))

        assert resp.status_code == 200, resp.text
        assert all(item["diversification"] is not None for item in resp.json()["items"])

    def test_mmr_penalties_reach_the_reranker(self):
        from ..lib.diversify import mmr_rerank

        gen_patch, lookup_patch, _ = _fake_generators(popularity=_candidates("pop", [0.9, 0.8]))
        rerank = MagicMock(side_effect=mmr_rerank)
        with gen_patch, lookup_patch, patch("app.lib.slate_pipeline.mmr_rerank", new=rerank):
            resp = _post(_body(diversify=True, author_penalty=0.2, topic_penalty=1.0))

        assert resp.status_code == 200, resp.text
        assert rerank.call_args.kwargs == {"author_penalty": 0.2, "topic_penalty": 1.0}

    def test_failed_generator_is_reported_as_degraded(self):
        gen_patch, lookup_patch, gens = _fake_generators(
            popularity=_candidates("pop", [0.5]), random_posts=[]
        )
        gens["random_posts"].generate.side_effect = RuntimeError("es down")
        with gen_patch, lookup_patch:
            resp = _post(
                _body(
                    generators=[
                        {"name": "popularity", "weight": 0.5},
                        {"name": "random_posts", "weight": 0.5},
                    ]
                )
            )

        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert [item["at_uri"] for item in data["items"]] == ["at://pop/0"]
        assert data["degraded"] == [{"stage": "candidate_gen", "component": "random_posts"}]

    def test_empty_slate_has_no_cursor_and_no_session(self):
        gen_patch, lookup_patch, _ = _fake_generators(popularity=[])
        with gen_patch, lookup_patch:
            resp = _post(_body())

        assert resp.status_code == 200, resp.text
        assert resp.json()["items"] == []
        assert resp.json()["cursor"] is None
        assert app.state.feed_cache._docs == {}

    def test_unknown_generator_and_ranker_are_404(self):
        gen_patch, lookup_patch, _ = _fake_generators(popularity=[])
        with gen_patch, lookup_patch:
            assert _post(_body(generators=[{"name": "nope"}])).status_code == 404
            assert _post(_body(infill="nope")).status_code == 404
            assert _post(_body(rankers=[{"name": "nope"}])).status_code == 404

    def test_pipeline_deadline_is_504(self, monkeypatch):
        monkeypatch.setenv("GE_FEED_REQUEST_TIMEOUT_SEC", "0.05")
        gen_patch, lookup_patch, gens = _fake_generators(popularity=[])

        async def hang(**kwargs):
            await asyncio.sleep(5)

        gens["popularity"].generate.side_effect = hang
        with gen_patch, lookup_patch:
            resp = _post(_body())
        assert resp.status_code == 504


# ---------------------------------------------------------------------------
# Request validation
# ---------------------------------------------------------------------------


class TestValidation:
    @pytest.mark.parametrize(
        "overrides",
        [
            {"cursor": "abc", "generators": [{"name": "popularity"}]},  # config with cursor
            {"generators": []},
            {"generators": [{"name": "external"}]},  # external without candidates
            {"external_candidates": [{"at_uri": "at://x/1"}]},  # candidates without external
            {"feed_name": "Not A Slug!"},
            {"rankers": []},
            {"author_penalty": 1.5},
            {"rankers": [{"name": "candidate_score", "weight": 0}]},
            {"min_rank_score": 0.5},  # needs rankers
            {"limit": 0},
            {"num_candidates": 201},
            {"unknown_field": 1},
        ],
    )
    def test_rejected_bodies(self, overrides):
        body = _body(**overrides)
        if "cursor" in overrides:
            keep = {"feed_name", "user_did", "limit", "cursor", "generators"}
            body = {k: v for k, v in body.items() if k in keep}
        assert _post(body).status_code == 422, overrides

    def test_requires_api_key(self):
        def deny():
            from fastapi import HTTPException

            raise HTTPException(status_code=401, detail="nope")

        app.dependency_overrides[verify_api_key] = deny
        try:
            assert _post(_body()).status_code == 401
        finally:
            app.dependency_overrides[verify_api_key] = lambda: "test-key-id"


# ---------------------------------------------------------------------------
# Cursor sessions
# ---------------------------------------------------------------------------


def _cursor_body(cursor: str, **overrides):
    body = {"feed_name": FEED_NAME, "user_did": USER_DID, "limit": 2, "cursor": cursor}
    body.update(overrides)
    return body


def _start_session(n: int = 5, limit: int = 2):
    gen_patch, lookup_patch, _ = _fake_generators(
        popularity=_candidates("pop", [1.0 - i / 10 for i in range(n)])
    )
    with gen_patch, lookup_patch:
        resp = _post(_body(limit=limit))
    assert resp.status_code == 200, resp.text
    return resp.json()


class TestCursor:
    def test_pages_through_the_cached_slate_then_ends(self):
        first = _start_session(n=5, limit=2)
        assert [i["at_uri"] for i in first["items"]] == ["at://pop/0", "at://pop/1"]
        assert first["cursor"] is not None

        second = _post(_cursor_body(first["cursor"]))
        assert second.status_code == 200, second.text
        assert [i["at_uri"] for i in second.json()["items"]] == ["at://pop/2", "at://pop/3"]
        assert second.json()["request_id"] == first["request_id"]
        assert second.json()["cursor"] is not None
        # Later pages carry the same metadata and a token for the same request.
        assert second.json()["items"][0]["generators"][0]["name"] == "popularity"
        payload = decode_feed_context(second.json()["items"][0]["feed_context"])
        assert payload is not None and payload.rid == first["request_id"]
        (diag,) = second.json()["generator_diagnostics"]
        assert diag["contributed_count"] == 2

        last = _post(_cursor_body(second.json()["cursor"]))
        assert last.status_code == 200, last.text
        assert [i["at_uri"] for i in last.json()["items"]] == ["at://pop/4"]
        assert last.json()["cursor"] is None

    def test_cursor_past_the_end_is_410_exhausted(self):
        first = _start_session(n=3, limit=2)
        parsed = FeedCursor.decode(first["cursor"])
        stale = FeedCursor(id=parsed.id, offset=3).encode()

        resp = _post(_cursor_body(stale))
        assert resp.status_code == 410
        assert resp.json()["detail"]["code"] == "cursor_exhausted"

    def test_unknown_session_is_410_expired(self):
        resp = _post(_cursor_body(FeedCursor(id="gone", offset=0).encode()))
        assert resp.status_code == 410
        assert resp.json()["detail"]["code"] == "cursor_expired"

    def test_malformed_cursor_is_400(self):
        assert _post(_cursor_body("not-a-cursor")).status_code == 400

    def test_cursor_is_bound_to_key_user_and_feed(self):
        first = _start_session()
        cursor = first["cursor"]

        assert _post(_cursor_body(cursor, user_did="did:plc:other")).status_code == 400
        assert _post(_cursor_body(cursor, feed_name="other-feed")).status_code == 400

        app.dependency_overrides[verify_api_key] = lambda: "other-key"
        try:
            assert _post(_cursor_body(cursor)).status_code == 400
        finally:
            app.dependency_overrides[verify_api_key] = lambda: "test-key-id"

    def test_feed_skeleton_cursors_are_not_honoured_here(self):
        """A getFeedSkeleton session (mode=served) must not be readable via the API."""
        from datetime import timedelta

        asyncio.run(
            app.state.feed_cache.store_document(
                "served1",
                FeedCacheDocument(
                    items=["at://a/1"],
                    expires_at=datetime.now(UTC) + timedelta(minutes=5),
                    user_did=USER_DID,
                    feed_name=FEED_NAME,
                    mode="served",
                ),
            )
        )
        resp = _post(_cursor_body(FeedCursor(id="served1", offset=0).encode()))
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Observability
# ---------------------------------------------------------------------------


class TestObservability:
    def test_metrics_carry_endpoint_and_caller_feed_name(self):
        from opentelemetry.sdk.metrics.export import InMemoryMetricReader

        reader = InMemoryMetricReader()
        set_metric_collector(MetricCollector._from_reader(reader, service_name="t", env="test"))
        try:
            _start_session(n=3, limit=2)
            metrics = {}
            metrics_data = reader.get_metrics_data()
            assert metrics_data is not None
            for rm in metrics_data.resource_metrics:
                for sm in rm.scope_metrics:
                    for metric in sm.metrics:
                        metrics[metric.name] = metric

            for name in (
                "feed.render.success_count",
                "feed.render.duration_ms",
                "feed.slate.kept_share",
            ):
                (point,) = metrics[name].data.data_points
                assert point.attributes["endpoint"] == "slate_generate", name
                assert point.attributes["feed_name"] == FEED_NAME, name
                assert point.attributes["traffic"] == "real", name
        finally:
            set_metric_collector(None)

    def test_failures_count_with_status(self):
        from opentelemetry.sdk.metrics.export import InMemoryMetricReader

        reader = InMemoryMetricReader()
        set_metric_collector(MetricCollector._from_reader(reader, service_name="t", env="test"))
        try:
            assert _post(_cursor_body("not-a-cursor")).status_code == 400
            metrics_data = reader.get_metrics_data()
            assert metrics_data is not None
            points = [
                point
                for rm in metrics_data.resource_metrics
                for sm in rm.scope_metrics
                for metric in sm.metrics
                if metric.name == "feed.render.failure_count"
                for point in metric.data.data_points
            ]
            (point,) = points
            assert point.attributes is not None
            assert point.attributes["status_code"] == "400"
            assert point.attributes["endpoint"] == "slate_generate"
            assert point.attributes["feed_name"] == FEED_NAME
        finally:
            set_metric_collector(None)

    def test_posthog_event_identifies_endpoint_key_and_feed(self):
        posthog = MagicMock()
        with patch("app.routers.slate.get_posthog_client", return_value=posthog):
            first = _start_session(n=3, limit=2)
            _post(_cursor_body(first["cursor"]))

        assert posthog.capture.call_count == 2
        fresh, paged = (c.kwargs for c in posthog.capture.call_args_list)
        assert fresh["distinct_id"] == USER_DID
        assert fresh["event"] == "slateGenerated"
        props = fresh["properties"]
        assert props["endpoint"] == "slate_generate"
        assert props["feed_name"] == FEED_NAME
        assert props["api_key_id"] == "test-key-id"
        assert props["generators"] == ["popularity"]
        assert props["rankers"] is None
        assert props["has_cursor"] is False
        assert props["item_count"] == 2
        assert paged["properties"]["has_cursor"] is True
        assert paged["properties"]["generators"] == []



# ---------------------------------------------------------------------------
# Settings previews
# ---------------------------------------------------------------------------


def _load(**overrides):
    """A fresh feed load (no cursor, not a preview)."""
    return _post(_body(**overrides))


def _preview(**overrides):
    return _post(_body(preview=True, **overrides))


def _with_generators(**by_name):
    """Context manager: canned generators; yields the generator mocks."""
    gen_patch, lookup_patch, gens = _fake_generators(**by_name)

    class _Ctx:
        def __enter__(self):
            gen_patch.__enter__()
            lookup_patch.__enter__()
            return gens

        def __exit__(self, *exc):
            lookup_patch.__exit__(*exc)
            gen_patch.__exit__(*exc)

    return _Ctx()


def _uris(response) -> list[str]:
    return [item["at_uri"] for item in response.json()["items"]]


class TestPreview:
    def test_settings_fingerprint_ignores_candidates_paging_and_preview(self):
        from ..models import SlateGenerateRequest

        base = SlateGenerateRequest.model_validate(_body())
        same_settings = SlateGenerateRequest.model_validate(
            _body(limit=3, exclude_uris=["at://x/1"], preview=True)
        )
        changed = SlateGenerateRequest.model_validate(_body(author_penalty=0.1))
        assert base.settings_fingerprint() == same_settings.settings_fingerprint()
        assert base.settings_fingerprint() != changed.settings_fingerprint()

    def test_preview_with_unchanged_settings_shows_the_last_feed_load(self):
        with _with_generators(popularity=_candidates("load", [0.9, 0.8, 0.7])):
            load = _load()
        assert load.json()["served_from"] == "generated"

        with _with_generators(popularity=_candidates("other", [0.9])) as gens:
            preview = _preview(limit=2)

        assert preview.status_code == 200, preview.text
        data = preview.json()
        assert data["served_from"] == "last_load"
        assert _uris(preview) == _uris(load)[:2]
        assert data["request_id"] != load.json()["request_id"]
        assert data["items"][0]["generators"] == load.json()["items"][0]["generators"]
        gens["popularity"].generate.assert_not_awaited()
        # Pageable like any session.
        page2 = _post(_cursor_body(data["cursor"], limit=5))
        assert page2.status_code == 200, page2.text
        assert page2.json()["served_from"] == "last_load"
        assert _uris(page2) == _uris(load)[2:]

    def test_saved_preview_is_served_to_the_next_load_with_its_settings(self):
        with _with_generators(popularity=_candidates("load", [0.9])):
            _load()
        with _with_generators(popularity=_candidates("pv", [0.9, 0.8, 0.7])):
            preview = _preview(author_penalty=0.2, limit=3)
        assert preview.json()["served_from"] == "generated"

        with _with_generators(popularity=_candidates("fresh", [0.9])) as gens:
            load = _load(author_penalty=0.2, limit=2)

        assert load.status_code == 200, load.text
        assert load.json()["served_from"] == "preview"
        assert load.json()["request_id"] == preview.json()["request_id"]
        assert _uris(load) == _uris(preview)[:2]
        gens["popularity"].generate.assert_not_awaited()
        page2 = _post(_cursor_body(load.json()["cursor"], limit=5))
        assert page2.json()["served_from"] == "preview"
        assert _uris(page2) == _uris(preview)[2:]

        # Served once; the load after that generates.
        with _with_generators(popularity=_candidates("fresh", [0.9])):
            again = _load(author_penalty=0.2)
        assert again.json()["served_from"] == "generated"

    def test_only_the_most_recent_preview_is_kept(self):
        with _with_generators(popularity=_candidates("a", [0.9])):
            _preview(author_penalty=0.2)
        with _with_generators(popularity=_candidates("b", [0.9])):
            _preview(author_penalty=0.9)
        with _with_generators(popularity=_candidates("fresh", [0.9])):
            load = _load(author_penalty=0.2)
        assert load.json()["served_from"] == "generated"
        assert _uris(load) == ["at://fresh/0"]

    def test_saving_different_settings_drops_the_preview(self, stored_slates):
        with _with_generators(popularity=_candidates("pv", [0.9])):
            _preview(author_penalty=0.2)
        with _with_generators(popularity=_candidates("fresh", [0.9])):
            load = _load(author_penalty=0.5)
        assert load.json()["served_from"] == "generated"
        assert not any(k[3] == "preview" for k in stored_slates.store)

    def test_returning_to_unchanged_settings_stages_nothing(self):
        with _with_generators(popularity=_candidates("load", [0.9])):
            _load()
        with _with_generators(popularity=_candidates("pv", [0.9])):
            _preview(author_penalty=0.2)  # try something
            back = _preview()  # back to the original settings
        assert back.json()["served_from"] == "last_load"

        # Leaving settings unchanged: the next load generates as usual.
        with _with_generators(popularity=_candidates("fresh", [0.9])):
            load = _load()
        assert load.json()["served_from"] == "generated"
        assert _uris(load) == ["at://fresh/0"]

    def test_an_empty_feed_load_replaces_the_last_load(self):
        """Review on #562: a nonempty load, then an empty one, then a preview of the
        same settings must show the empty feed, not the older nonempty slate."""
        with _with_generators(popularity=_candidates("load", [0.9, 0.8])):
            assert _uris(_load()) == ["at://load/0", "at://load/1"]
        with _with_generators(popularity=[]):
            assert _uris(_load()) == []

        with _with_generators(popularity=_candidates("pv", [0.9])) as gens:
            preview = _preview()

        assert preview.status_code == 200, preview.text
        assert preview.json()["served_from"] == "last_load"
        assert _uris(preview) == []
        assert preview.json()["cursor"] is None
        gens["popularity"].generate.assert_not_awaited()

    def test_feed_loads_become_the_last_load(self, stored_slates):
        with _with_generators(popularity=_candidates("pv", [0.9])):
            preview = _preview(author_penalty=0.2)
        with _with_generators(popularity=[]):
            _load(author_penalty=0.2)  # served the preview
        last = stored_slates.store[(USER_DID, "test-key-id", FEED_NAME, "last_load")]
        assert last.request_id == preview.json()["request_id"]
        assert last.slate.items == _uris(preview)

    def test_preview_has_no_side_effects_on_history(self, discarded):
        with (
            _with_generators(popularity=_candidates("pv", [0.9, 0.1])) as gens,
            patch(
                "app.routers.xrpc.get_recent_seen_uris",
                new_callable=AsyncMock,
                return_value=["at://seen/1"],
            ),
            patch(
                "app.routers.xrpc.get_recent_discarded_uris",
                new_callable=AsyncMock,
                return_value=["at://discarded/1"],
            ),
        ):
            resp = _preview(
                rankers=[{"name": "candidate_score", "weight": 1.0}], min_rank_score=0.5
            )

        assert resp.status_code == 200, resp.text
        assert _uris(resp) == ["at://pv/0"]
        # Seen posts stay eligible; earlier low-score discards stay excluded.
        assert gens["popularity"].generate.await_args.kwargs["exclude_uris"] == [
            "at://discarded/1"
        ]
        discarded.assert_not_called()

    def test_empty_preview_is_not_handed_to_the_feed(self, stored_slates):
        with _with_generators(popularity=_candidates("pv", [0.9])):
            _preview(author_penalty=0.2)
        with _with_generators(popularity=[]):
            empty = _preview(author_penalty=0.2)
        assert empty.json()["items"] == []
        assert not any(k[3] == "preview" for k in stored_slates.store)

    def test_claim_failure_falls_back_to_generating(self, stored_slates):
        stored_slates.claim.side_effect = RuntimeError("firestore down")
        with _with_generators(popularity=_candidates("fresh", [0.5])):
            resp = _load()
        assert resp.status_code == 200, resp.text
        assert resp.json()["served_from"] == "generated"

    def test_preview_traffic_metrics_and_analytics(self):
        from opentelemetry.sdk.metrics.export import InMemoryMetricReader

        with _with_generators(popularity=_candidates("pv", [0.9])):
            _preview(author_penalty=0.2)
        reader = InMemoryMetricReader()
        set_metric_collector(MetricCollector._from_reader(reader, service_name="t", env="test"))
        posthog = MagicMock()
        try:
            with (
                _with_generators(popularity=_candidates("pv", [0.9])),
                patch("app.routers.slate.get_posthog_client", return_value=posthog),
            ):
                _preview(author_penalty=0.9)
                _load(author_penalty=0.9)
            metrics_data = reader.get_metrics_data()
            assert metrics_data is not None
            points = {
                (metric.name, point.attributes.get("traffic")): point
                for rm in metrics_data.resource_metrics
                for sm in rm.scope_metrics
                for metric in sm.metrics
                for point in metric.data.data_points
                if point.attributes is not None
            }
        finally:
            set_metric_collector(None)

        assert ("feed.render.success_count", "preview") in points
        assert ("feed.render.success_count", "real") in points
        claim = points[("feed.slate_preview.claim_count", "real")]
        assert claim.attributes is not None
        assert claim.attributes["outcome"] == "served"
        events = [c.kwargs["properties"] for c in posthog.capture.call_args_list]
        assert [(e["preview"], e["served_from"]) for e in events] == [
            (True, "generated"),
            (False, "preview"),
        ]
