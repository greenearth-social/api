"""Offline coverage of cohort completeness, failure policy, and saved means."""

import json
import logging
import math
import subprocess
import sys
import threading
from collections import Counter
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import average_user_embedding as average
import httpx
import pytest


# Most cases replace a service boundary with deterministic responses. The companion
# integration test additionally connects the producer to the real embedding router.
class FakeClient(httpx.Client):
    def __init__(self, respond):
        self.requests = []

        def handle(request):
            payload = json.loads(request.content)
            self.requests.append((request.url.path, payload))
            result = respond(request.url.path, payload)
            if isinstance(result, httpx.Response):
                return result
            # Raw JSON permits malformed numeric values for validation tests.
            return httpx.Response(200, content=json.dumps(result))

        super().__init__(base_url="https://unused", transport=httpx.MockTransport(handle))


def es_response(counts):
    return {
        "timed_out": False,
        "_shards": {"total": 3, "successful": 3, "failed": 0},
        "aggregations": {
            "users": {
                "sum_other_doc_count": 0,
                "doc_count_error_upper_bound": 0,
                "buckets": [{"key": did, "doc_count": count} for did, count in counts.items()],
            }
        },
    }


def user(did, interactions=50, likes=5):
    return {"user_did": did, "interaction_seen_count": interactions, "like_count": likes}


USER_MODEL = "1affd684bc7f45f895e488f83dd0a2fa"
POST_MODEL = "9b946f280fd84899a7f82246fbc34d17"
POLICY = {"limit": 64, "sources": ["posts", "replies"], "embedding_key": "all_MiniLM_L12_v2"}


def embedding(did, vector=None, model=USER_MODEL):
    if vector is None:
        vector = [2.0, 4.0]
    return {
        "user_did": did,
        "status": "ok",
        "embedding": vector,
        "dimension": len(vector),
        "user_model_uuid": model,
        "post_model_uuid": POST_MODEL,
        "history_policy": POLICY.copy(),
        "likes_index": "likes",
        "history_like_count": 5,
        "history_embedding_count": 3,
        "reason": None,
    }


def skipped(did, reason="no_embedded_history"):
    return {
        "user_did": did,
        "status": "skipped",
        "reason": reason,
        "history_policy": POLICY.copy(),
        "likes_index": "likes",
        "history_like_count": 0 if reason == "no_likes" else 5,
        "history_embedding_count": 0,
    }


def test_posthog_paginates_past_short_pages_with_fixed_cutoff_and_full_counts():
    # Return only 100 rows despite LIMIT 1000: stopping on a short page would lose
    # most users, while recomputing page-local counts would lose their full activity.
    expected = {f"did:plc:u{number:04d}": 50 + number for number in range(2305)}

    def respond(path, payload):
        assert path == "/api/projects/509275/query/"
        query = payload["query"]
        assert query["values"]["minimum"] == 50
        assert query["values"]["cutoff"] == "2026-09-17T00:00:00Z"
        assert query["values"]["page_size"] == 1000
        assert "HAVING count() >= {minimum}" in query["query"]
        assert "LIMIT {page_size}" in query["query"]
        assert "feed_name" not in query["query"]
        assert "timestamp >=" not in query["query"]
        assert payload["refresh"] == "force_blocking"
        cursor = query["values"]["after_did"]
        return {"results": [[did, count] for did, count in expected.items() if did > cursor][:100]}

    client = FakeClient(respond)
    assert average.collect_posthog_users(client, 509275, 50, "2026-09-17T00:00:00Z") == expected
    assert len(client.requests) == 25


@pytest.mark.parametrize(
    "result",
    [
        {"query_status": {"complete": False}},
        {"results": [], "error": "failure"},
        {"results": [["not-a-did", 50]]},
        {"results": [["did:plc:a", 49]]},
        {"results": [["did:plc:a", True]]},
    ],
)
def test_posthog_rejects_incomplete_or_invalid_data(result):
    with pytest.raises(average.RunError):
        average.collect_posthog_users(FakeClient(lambda *_: result), 1, 50, "cutoff")


def test_posthog_stalled_pagination_is_fatal():
    with pytest.raises(average.RunError, match="did not advance"):
        average.collect_posthog_users(
            FakeClient(lambda *_: {"results": [["did:plc:a", 50]]}), 1, 50, "cutoff"
        )


def test_likes_batching_is_exact_and_missing_users_count_zero():
    # Two full batches plus a one-user tail exercise both aggregation size limits.
    # The omitted first DID represents a user with no retained like documents.
    dids = [f"did:plc:u{number:04d}" for number in range(1001)]

    def respond(path, payload):
        assert path == "/likes/_search"
        batch = payload["query"]["terms"]["author_did"]
        terms = payload["aggs"]["users"]["terms"]
        assert terms["field"] == "author_did"
        assert terms["size"] == terms["shard_size"] == len(batch)
        return es_response({did: 5 for did in batch if did != dids[0]})

    client = FakeClient(respond)
    result = average.collect_like_counts(client, dids)
    assert result == {did: 0 if did == dids[0] else 5 for did in dids}
    assert [len(payload["query"]["terms"]["author_did"]) for _, payload in client.requests] == [
        500,
        500,
        1,
    ]


@pytest.mark.parametrize(
    "mutation",
    [
        lambda data: data.update(timed_out=True),
        lambda data: data["_shards"].update(failed=1),
        lambda data: data["_shards"].update(successful=2),
        lambda data: data.update(terminated_early=True),
        lambda data: data["aggregations"]["users"].update(sum_other_doc_count=1),
        lambda data: data["aggregations"]["users"].update(doc_count_error_upper_bound=1),
        lambda data: data["aggregations"]["users"]["buckets"][0].update(
            doc_count_error_upper_bound=1
        ),
        lambda data: data.update(_shards=None),
        lambda data: data.update(aggregations=[]),
        lambda data: data["aggregations"].update(users=None),
    ],
)
def test_likes_rejects_partial_or_inexact_aggregations(mutation):
    data = es_response({"did:plc:a": 5})
    mutation(data)
    with pytest.raises(average.RunError):
        average.collect_like_counts(FakeClient(lambda *_: data), ["did:plc:a"])


def test_post_json_sends_payload_and_returns_object():
    with FakeClient(lambda *_: {"ok": True}) as client:
        assert average.post_json(client, "/query", {"limit": 10}) == {"ok": True}
        assert client.requests == [("/query", {"limit": 10})]


@pytest.mark.parametrize("status", [302, 400, 401, 403, 404, 429, 500, 502, 504])
def test_http_errors_are_not_retried_or_redirected(status):
    def respond(*_):
        return httpx.Response(
            status,
            headers={"Retry-After": "7", "Location": "https://another-host.test"},
            json={"detail": {"code": "upstream_error", "message": "NEVER-PUBLISH-THIS-SECRET"}},
        )

    with FakeClient(respond) as client:
        with pytest.raises(httpx.HTTPStatusError) as error:
            average.post_json(client, "/embeddings/user", {"user_did": "did:plc:a"})
        assert error.value.response.status_code == status
        assert "NEVER-PUBLISH" not in str(error.value)
        assert len(client.requests) == 1


@pytest.mark.parametrize(
    "error_type", [httpx.ReadTimeout, httpx.ConnectError, httpx.RemoteProtocolError]
)
def test_transport_errors_stop_after_one_attempt(error_type):
    def respond(*_):
        raise error_type("Transport failed")

    with FakeClient(respond) as client:
        with pytest.raises(error_type):
            average.post_json(client, "/embeddings/user", {})
        assert len(client.requests) == 1


@pytest.mark.parametrize("body", [b"not JSON", b"", b"[]", b"null"])
def test_invalid_json_response_reports_the_url(body):
    with FakeClient(lambda *_: httpx.Response(200, content=body)) as client:
        with pytest.raises(average.RunError, match="https://unused/query"):
            average.post_json(client, "/query", {})


@pytest.mark.parametrize(
    "vector", [[], [float("nan")], [float("inf")], [10**1000], [True], ["1"], [[1]]]
)
def test_invalid_vectors_are_rejected(vector):
    client = FakeClient(lambda *_: embedding("did:plc:a", vector))
    with pytest.raises(average.RunError, match="finite nonempty vector"):
        average.fetch_embedding(client, "did:plc:a")


@pytest.mark.parametrize("reason", ["no_likes", "no_embedded_history"])
def test_missing_history_is_explicitly_skipped(reason):
    result = average.fetch_embedding(
        FakeClient(lambda *_: skipped("did:plc:a", reason)), "did:plc:a"
    )
    assert result == {"status": "skipped", "reason": reason, "history_policy": POLICY}


@pytest.mark.parametrize(
    "response",
    [
        {**skipped("did:plc:a", "no_likes"), "history_like_count": 1},
        {**skipped("did:plc:a"), "history_embedding_count": 1},
        {**embedding("did:plc:a"), "history_embedding_count": 6},
        {**embedding("did:plc:a"), "history_like_count": 65},
    ],
)
def test_inconsistent_history_counts_do_not_contribute(response):
    with pytest.raises(average.RunError, match="counts"):
        average.fetch_embedding(FakeClient(lambda *_: response), "did:plc:a")


def average_users(client, users, workers=2):
    # Activity/like counts qualify users upstream; only DIDs enter the mean stage.
    return average.average_embeddings(client, [record["user_did"] for record in users], workers)


def test_mean_is_equal_weight_then_normalized_and_returns_only_aggregate_metadata():
    responses = {
        "did:plc:a": embedding("did:plc:a", [2, 4]),
        "did:plc:b": embedding("did:plc:b", [6, 8]),
    }
    client = FakeClient(lambda _, payload: responses[payload["user_did"]])
    result = average_users(client, [user("did:plc:a", 50, 5), user("did:plc:b", 500, 50)])
    # Unequal input magnitudes distinguish normalizing the mean from averaging
    # individually normalized inputs. Interaction and like counts do not weight it.
    assert result["embedding"] == pytest.approx([4 / math.sqrt(52), 6 / math.sqrt(52)])
    assert math.hypot(*result["embedding"]) == pytest.approx(1)
    assert result["dimension"] == 2
    assert result["user_model_uuid"] == USER_MODEL
    assert result["post_model_uuid"] == POST_MODEL
    assert result["contributing_users"] == 2
    assert result["skipped_users"] == 0
    assert set(result) == {
        "embedding",
        "dimension",
        "user_model_uuid",
        "post_model_uuid",
        "history_policy",
        "contributing_users",
        "skipped_users",
    }
    assert "did:" not in json.dumps(result)
    assert Counter(payload["user_did"] for _, payload in client.requests) == {
        "did:plc:a": 1,
        "did:plc:b": 1,
    }


def test_single_contributor_is_normalized():
    result = average_users(
        FakeClient(lambda *_: embedding("did:plc:a", [3, 4])), [user("did:plc:a")]
    )
    assert result["embedding"] == pytest.approx([0.6, 0.8])
    assert result["contributing_users"] == 1


def test_missing_history_updates_aggregate_counts_and_logs_without_retaining_dids(caplog):
    caplog.set_level(logging.INFO, logger=average.__name__)
    responses = {
        "did:plc:a": embedding("did:plc:a", [2, 4]),
        "did:plc:b": skipped("did:plc:b", "no_likes"),
        "did:plc:c": skipped("did:plc:c", "no_embedded_history"),
    }
    client = FakeClient(lambda _, payload: responses[payload["user_did"]])
    result = average_users(client, [user(did) for did in responses])
    assert result["embedding"] == pytest.approx([2 / math.sqrt(20), 4 / math.sqrt(20)])
    assert result["contributing_users"] == 1
    assert result["skipped_users"] == 2
    assert "summary eligible=3 contributing=1 skipped=2" in caplog.text
    assert "skipped reason=no_likes count=1" in caplog.text
    assert "skipped reason=no_embedded_history count=1" in caplog.text
    assert "did:" not in caplog.text + json.dumps(result)


@pytest.mark.parametrize(
    "second,match",
    [
        (embedding("did:plc:b", model="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), "mixed model"),
        (
            {**embedding("did:plc:b"), "post_model_uuid": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
            "mixed model",
        ),
        (embedding("did:plc:b", [1, 2, 3]), "mixed model"),
        ({**embedding("did:plc:b"), "history_policy": {**POLICY, "limit": 50}}, "mixed history"),
        ({**skipped("did:plc:b"), "likes_index": "other-index"}, "source mismatch"),
    ],
)
def test_metadata_mismatch_is_global_failure(second, match):
    # Equal-length vectors are still incompatible if their model pair or source
    # history differs; these failures invalidate the whole run, not just one user.
    client = FakeClient(
        lambda _, payload: embedding("did:plc:a") if payload["user_did"] == "did:plc:a" else second
    )
    with pytest.raises(average.RunError, match=match):
        average_users(client, [user("did:plc:a"), user("did:plc:b")])


@pytest.mark.parametrize(
    "field,value,reason",
    [
        ("history_policy", None, "Invalid history policy metadata"),
        ("history_policy", {**POLICY, "limit": True}, "Invalid history policy metadata"),
        ("user_model_uuid", "invalid", "Model identifiers must be nonzero UUIDs"),
        ("post_model_uuid", None, "Model identifiers must be nonzero UUIDs"),
    ],
)
def test_shared_metadata_validation_preserves_failure_reasons(field, value, reason):
    client = FakeClient(lambda *_: {**embedding("did:plc:a"), field: value})
    with pytest.raises(average.ArtifactValidationError, match=reason):
        average_users(client, [user("did:plc:a")])


def test_zero_contributors_is_fatal():
    with pytest.raises(average.RunError, match="No valid"):
        average_users(FakeClient(lambda *_: skipped("did:plc:a")), [user("did:plc:a")])
    with pytest.raises(average.RunError, match="No valid"):
        average_users(FakeClient(lambda *_: None), [])


def test_zero_magnitude_mean_is_fatal():
    client = FakeClient(
        lambda _, p: embedding(p["user_did"], [1, -1] if p["user_did"].endswith("a") else [-1, 1])
    )
    with pytest.raises(average.RunError, match="nonzero"):
        average_users(client, [user("did:plc:a"), user("did:plc:b")])


@pytest.mark.parametrize(
    "vector,users",
    [([1e308], ["a", "b"]), ([1.7e308, 1.7e308], ["a"])],
    ids=["sum-overflow", "magnitude-overflow"],
)
def test_nonfinite_aggregation_is_fatal(vector, users):
    client = FakeClient(lambda _, p: embedding(p["user_did"], vector))
    with pytest.raises(average.RunError, match="finite"):
        average_users(client, [user(f"did:plc:{name}") for name in users])


def test_single_failed_user_stops_submissions_and_prevents_partial_average(caplog):
    caplog.set_level(logging.INFO, logger=average.__name__)

    def respond(_, payload):
        if payload["user_did"] == "did:plc:b":
            raise httpx.ReadTimeout("Embedding request timed out")
        return embedding(payload["user_did"])

    client = FakeClient(respond)
    with pytest.raises(httpx.ReadTimeout):
        average_users(client, [user(f"did:plc:{letter}") for letter in "abc"], workers=1)
    assert len(client.requests) == 2
    assert "Average: computed" not in caplog.text
    assert "did:plc:" not in caplog.text


def test_global_failure_stops_submissions_and_drains_active_requests():
    # Synchronize the first four workers so the test does not depend on scheduling.
    # A fatal error should prevent any of the remaining 996 requests from starting.
    barrier = threading.Barrier(4)

    def respond(*_):
        barrier.wait(timeout=5)
        raise average.RunError("global configuration failure")

    client = FakeClient(respond)
    with pytest.raises(average.RunError, match="configuration"):
        average_users(client, [user(f"did:plc:u{i}") for i in range(1000)], workers=4)
    assert len(client.requests) == 4


def install_pipeline_fakes(monkeypatch, responder=None, collection_failure=False):
    # Exercise real HTTPX clients without sockets; keep references to verify closure.
    for name in ("POSTHOG_PERSONAL_API_KEY", "GE_ELASTICSEARCH_API_KEY", "GE_API_KEY"):
        monkeypatch.setenv(name, "SECRET-" + name)
    clients = []
    real_client = httpx.Client

    def make_client(**kwargs):
        requests = []
        authorization = kwargs["headers"].get("Authorization", "")

        def handle(request):
            payload = json.loads(request.content)
            requests.append((request.url.path, payload))
            if authorization.startswith("Bearer "):
                if collection_failure:
                    return httpx.Response(502, text="NEVER-PUBLISH-THIS-SECRET")
                result = {
                    "results": []
                    if payload["query"]["values"]["after_did"]
                    else [
                        [f"did:plc:{letter}", 50 + number] for number, letter in enumerate("abcdef")
                    ]
                }
            elif authorization.startswith("ApiKey "):
                result = es_response({f"did:plc:{letter}": 5 for letter in "abcde"})
            else:
                did = payload["user_did"]
                result = (
                    responder(did)
                    if responder is not None
                    else skipped(did)
                    if did == "did:plc:c"
                    else embedding(did, [2, 4] if did == "did:plc:a" else [6, 8])
                )
            if isinstance(result, httpx.Response):
                return result
            return httpx.Response(200, content=json.dumps(result))

        client = real_client(**kwargs, transport=httpx.MockTransport(handle))
        clients.append(SimpleNamespace(client=client, config=kwargs, requests=requests))
        return client

    monkeypatch.setattr(average.httpx, "Client", make_client)
    return clients


def read_summary(capsys):
    captured = capsys.readouterr()
    assert "SECRET-" not in captured.out + captured.err
    return json.loads(captured.out)


@pytest.mark.parametrize(
    "tls_args,insecure", [([], True), (["--es-insecure"], True), (["--no-es-insecure"], False)]
)
def test_full_pipeline_saves_only_compact_artifact_and_no_secrets(
    tmp_path, monkeypatch, capsys, tls_args, insecure
):
    # Drive the real CLI, validation, and atomic writer; only external clients are
    # replaced. The output directory must contain the single consumer artifact.
    clients = install_pipeline_fakes(monkeypatch)
    assert average.main(["--output-dir", str(tmp_path / "output"), *tls_args]) == 0
    summary = read_summary(capsys)
    artifact_path = Path(summary["artifact_path"])
    artifact = json.loads(artifact_path.read_text())
    assert average.validate_artifact(artifact) == artifact
    assert artifact["format_version"] == 1
    assert artifact["embedding"] == pytest.approx([5 / math.sqrt(74), 7 / math.sqrt(74)])
    assert artifact_path.name == f"average_user_embedding_{artifact['run_id']}.json"
    assert artifact["run_id"] == summary["run_id"]
    assert artifact["cohort"]["min_likes"] == 5
    assert summary["status"] == "success"
    assert set(summary) == {"status", "artifact_path", "run_id"}
    assert artifact["contributing_users"] == 4
    assert {
        key: artifact["cohort"][key]
        for key in (
            "posthog_users",
            "below_min_likes",
            "eligible_users",
            "skipped_users",
        )
    } == {
        "posthog_users": 6,
        "below_min_likes": 1,
        "eligible_users": 5,
        "skipped_users": 1,
    }
    artifact_text = artifact_path.read_text()
    assert "did:" not in artifact_text and "http" not in artifact_text
    assert artifact_text.count('"embedding"') == 1
    assert [record.config.get("verify", True) for record in clients] == [
        True,
        not insecure,
        True,
    ]
    assert all(record.client.is_closed for record in clients)
    assert all(record.client.timeout == httpx.Timeout(60) for record in clients)
    assert all(not record.client.follow_redirects for record in clients)
    assert clients[0].client.headers["Authorization"] == "Bearer SECRET-POSTHOG_PERSONAL_API_KEY"
    assert clients[1].client.headers["Authorization"] == "ApiKey SECRET-GE_ELASTICSEARCH_API_KEY"
    assert clients[2].client.headers["X-API-Key"] == "SECRET-GE_API_KEY"
    assert Counter(payload["user_did"] for _, payload in clients[2].requests) == {
        f"did:plc:{letter}": 1 for letter in "abcde"
    }
    assert list((tmp_path / "output").iterdir()) == [artifact_path]
    for path in (tmp_path / "output").iterdir():
        assert "SECRET-" not in path.read_text()
    assert not list((tmp_path / "output").glob(".average*"))


@pytest.mark.parametrize(
    "failure", ["zero", "auth", "mixed", "request", "malformed", "json", "collection", "interrupt"]
)
def test_failed_generation_returns_error_without_output(
    tmp_path, monkeypatch, capsys, caplog, failure
):
    # Failure may occur at different stages, but no path may leave a usable-looking
    # artifact or expose individual DIDs and credentials in the summary.
    caplog.set_level(logging.INFO, logger=average.__name__)

    def respond(did):
        if failure == "zero":
            return skipped(did)
        if failure == "auth":
            return httpx.Response(401, text="NEVER-PUBLISH-THIS-SECRET")
        if failure == "interrupt":
            raise KeyboardInterrupt
        if failure == "request":
            raise httpx.ReadTimeout("Embedding request timed out")
        if failure == "json":
            return httpx.Response(200, content="not JSON")
        if failure == "malformed":
            return embedding(did, [float("nan")])
        return embedding(
            did, model=USER_MODEL if did == "did:plc:a" else "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        )

    clients = install_pipeline_fakes(
        monkeypatch, respond, collection_failure=failure == "collection"
    )
    assert average.main(["--output-dir", str(tmp_path)]) == 1
    summary = read_summary(capsys)
    assert summary["artifact_path"] is None
    assert summary["status"] == "failed" and summary["error"]
    assert all(record.client.is_closed for record in clients)
    if failure in ("auth", "request", "json"):
        assert "http://localhost:8300/embeddings/user" in summary["error"]
    if failure == "auth":
        assert "401" in summary["error"]
    if failure == "request":
        assert "ReadTimeout" in summary["error"]
    assert "NEVER-PUBLISH" not in json.dumps(summary) + caplog.text
    assert set(summary) == {"status", "artifact_path", "error"}
    assert not list(tmp_path.iterdir())
    assert "did:" not in caplog.text
    assert "SECRET-" not in caplog.text


@pytest.mark.parametrize("override", [None, "custom/output", "~/embedding-results"])
def test_output_paths_expand_from_cwd_or_home(tmp_path, monkeypatch, capsys, caplog, override):
    caplog.set_level(logging.INFO, logger=average.__name__)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    install_pipeline_fakes(monkeypatch)
    argv = [] if override is None else ["--output-dir", override]
    assert average.main(argv) == 0
    summary = read_summary(capsys)
    expected = tmp_path / (
        "outputs/average_user_embeddings"
        if override is None
        else "home/embedding-results"
        if override.startswith("~")
        else override
    )
    assert Path(summary["artifact_path"]).parent == expected
    assert expected.is_dir()
    assert f"output_dir={expected}" in caplog.text
    assert list(expected.iterdir()) == [Path(summary["artifact_path"])]


def test_local_output_is_atomic_and_cleans_failed_write(tmp_path, monkeypatch):
    # Fail at the final rename, after JSON serialization, to check temporary cleanup.
    target = tmp_path / "artifact.json"

    def fail_write(source, destination):
        assert json.loads(source.read_text()) == {"embedding": [1, 2]}
        assert not destination.exists()
        raise OSError("simulated failure")

    monkeypatch.setattr(average.os, "replace", fail_write)
    with pytest.raises(OSError):
        average.atomic_json(target, {"embedding": [1, 2]})
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    "url",
    [
        "https://user:secret@example.com",
        "https://example.com?key=secret",
        "https://example.com#secret",
        "ftp://example.com",
        "https://example.com:bad",
    ],
)
def test_urls_cannot_embed_credentials_or_secret_query_values(url):
    with pytest.raises(SystemExit):
        average.build_parser().parse_args(["--api-url", url])


def test_missing_credentials_fail_before_network_without_output(tmp_path, monkeypatch, capsys):
    monkeypatch.delenv("POSTHOG_PERSONAL_API_KEY", raising=False)
    monkeypatch.setattr(
        average.httpx, "Client", Mock(side_effect=AssertionError("network must not start"))
    )
    assert average.main(["--output-dir", str(tmp_path)]) == 1
    summary = read_summary(capsys)
    assert "POSTHOG_PERSONAL_API_KEY" in summary["error"]
    assert summary["artifact_path"] is None
    assert summary["status"] == "failed"
    assert not list(tmp_path.iterdir())


def test_embedding_logs_are_aggregate_and_safe(tmp_path, monkeypatch, caplog, capsys):
    # Run the CLI so HTTPX request logging is configured too. Per-user vectors and
    # request lines should stay out of logs, while the aggregate remains informative.
    caplog.set_level(logging.INFO)
    vector = [123456.789, -987654.321]
    install_pipeline_fakes(monkeypatch, lambda did: embedding(did, vector))
    assert average.main(["--output-dir", str(tmp_path)]) == 0
    read_summary(capsys)
    assert USER_MODEL in caplog.text and POST_MODEL in caplog.text
    assert "dimension=2" in caplog.text
    assert "summary eligible=5 contributing=5 skipped=0" in caplog.text
    assert "did:plc:" not in caplog.text
    assert "HTTP Request:" not in caplog.text
    assert all(str(value) not in caplog.text for value in vector)
    assert "L2-normalized unweighted mean" in caplog.text


def test_defaults_are_generic_and_local_only():
    args = average.build_parser().parse_args([])
    assert args.es_url == "https://localhost:9200"
    assert args.es_insecure is True
    assert args.api_url == "http://localhost:8300"
    assert args.output_dir == "./outputs/average_user_embeddings/"


def test_help_works_from_another_directory(tmp_path):
    result = subprocess.run(
        [sys.executable, str(Path(average.__file__).resolve()), "--help"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
    assert "--output-dir" in result.stdout
    assert "--publish-artifact" not in result.stdout
    assert "--gcs-output-prefix" not in result.stdout
    assert "--no-es-insecure" in result.stdout


@pytest.mark.parametrize(
    "flag,value",
    [("--publish-artifact", "artifact.json"), ("--gcs-output-prefix", "gs://test-bucket/averages")],
)
def test_removed_upload_options_fail_before_generation(tmp_path, monkeypatch, capsys, flag, value):
    generate = Mock(side_effect=AssertionError("generation must not start"))
    monkeypatch.setattr(average, "generate", generate)
    with pytest.raises(SystemExit) as error:
        average.main(["--output-dir", str(tmp_path), flag, value])
    assert error.value.code == 2
    assert "unrecognized arguments" in capsys.readouterr().err
    generate.assert_not_called()
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize(
    "mutation",
    [
        lambda data: data["_shards"].update(failed=False),
        lambda data: data["aggregations"]["users"].update(sum_other_doc_count=False),
        lambda data: data["aggregations"]["users"].update(doc_count_error_upper_bound=False),
        lambda data: data["aggregations"]["users"]["buckets"][0].update(
            doc_count_error_upper_bound=False
        ),
        lambda data: data.update(terminated_early=0),
    ],
)
def test_likes_reject_bool_metadata(mutation):
    result = es_response({"did:plc:a": 5})
    mutation(result)
    with pytest.raises(average.RunError):
        average.collect_like_counts(FakeClient(lambda *_: result), ["did:plc:a"])


@pytest.mark.parametrize("interrupt", ["wait", "request"])
def test_interrupt_stops_submissions_and_drains_active_requests(monkeypatch, interrupt):
    # Cover Ctrl-C in the coordinating thread and an interrupt raised by a worker.
    # The initial two jobs may finish, but cancellation must not enqueue more users.
    if interrupt == "wait":
        original = average.wait
        calls = 0

        def wait_once_interrupted(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise KeyboardInterrupt
            return original(*args, **kwargs)

        monkeypatch.setattr(average, "wait", wait_once_interrupted)

    def respond(_, payload):
        if interrupt == "request":
            raise KeyboardInterrupt
        return embedding(payload["user_did"])

    client = FakeClient(respond)
    with pytest.raises(KeyboardInterrupt):
        average_users(client, [user(f"did:plc:u{i}") for i in range(10)], workers=2)
    assert len(client.requests) == 2
