"""Offline coverage of cohort completeness, failure policy, and published means."""

import io
import json
import logging
import threading
from collections import Counter
from http.client import IncompleteRead
from pathlib import Path
from unittest.mock import Mock
from urllib.error import HTTPError, URLError

import average_user_embedding as average
import pytest


class FakeClient:
    def __init__(self, respond):
        self.respond = respond
        self.requests = []

    def post(self, path, payload):
        self.requests.append((path, payload))
        return self.respond(path, payload)


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
SOURCE = {"es_cluster_uuid": "es-cluster-A", "likes_index": "likes"}
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
        **SOURCE,
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
        **SOURCE,
        "history_like_count": 0 if reason == "no_likes" else 5,
        "history_embedding_count": 0,
    }


def test_posthog_paginates_past_short_pages_with_fixed_cutoff_and_full_counts():
    expected = {f"did:plc:u{number:04d}": 50 + number for number in range(2305)}

    def respond(path, payload):
        assert path == "/api/projects/509275/query/"
        query = payload["query"]
        assert query["values"]["minimum"] == 50
        assert query["values"]["cutoff"] == "2026-09-17T00:00:00Z"
        assert "HAVING count() >= {minimum}" in query["query"]
        assert "LIMIT 1000" in query["query"]
        assert "feed_name" not in query["query"]
        assert "timestamp >=" not in query["query"]
        assert payload["refresh"] == "force_blocking"
        cursor = query["values"]["after_did"]
        return {"results": [[did, count] for did, count in expected.items() if did > cursor][:100]}

    client = FakeClient(respond)
    assert average.collect_posthog_users(client, 509275, 50, "2026-09-17T00:00:00Z") == expected
    assert len(client.requests) == 25


def test_posthog_deduplicates_without_summing_counts():
    pages = iter(
        [
            [["did:plc:a", 50], ["did:plc:a", 50], ["did:plc:b", 51]],
            [["did:plc:b", 51], ["did:plc:c", 52]],
            [],
        ]
    )
    client = FakeClient(lambda *_: {"results": next(pages)})
    assert average.collect_posthog_users(client, 1, 50, "cutoff") == {
        "did:plc:a": 50,
        "did:plc:b": 51,
        "did:plc:c": 52,
    }


@pytest.mark.parametrize(
    "result",
    [
        {"query_status": {"complete": False}},
        {"results": [], "error": "failure"},
        {"results": [["not-a-did", 50]]},
        {"results": [["did:plc:a", 49]]},
        {"results": [["did:plc:a", True]]},
        {"results": [["did:plc:b", 50], ["did:plc:a", 51]]},
        {"results": [["did:plc:a", 50], ["did:plc:a", 51]]},
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
    dids = [f"did:plc:u{number:04d}" for number in range(1001)]

    def respond(path, payload):
        assert path == "/likes/_search"
        batch = payload["query"]["terms"]["author_did"]
        terms = payload["aggs"]["users"]["terms"]
        assert terms["field"] == "author_did"
        assert terms["size"] == terms["shard_size"] == len(batch)
        return es_response({did: 5 for did in batch if did != dids[0]})

    client = FakeClient(respond)
    result = average.collect_like_counts(client, "likes", dids)
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
        average.collect_like_counts(FakeClient(lambda *_: data), "likes", ["did:plc:a"])


def http_error(status, retry_after=None, code=None):
    headers = {"Retry-After": retry_after} if retry_after is not None else {}
    body = {"detail": {"code": code, "message": "NEVER-PUBLISH-THIS-SECRET"}}
    return HTTPError(
        "https://unused", status, "error", headers, io.BytesIO(json.dumps(body).encode())
    )


def client_with_responses(monkeypatch, responses):
    client = average.JsonClient("Embedding API", "https://unused", {"X-API-Key": "private-key"})
    opener = Mock()
    opener.open.side_effect = responses
    client.opener = opener
    sleep = Mock()
    monkeypatch.setattr(average.time, "sleep", sleep)
    return client, opener, sleep


def test_client_retries_rate_limit_and_network_error_then_returns_json(monkeypatch):
    client, opener, sleep = client_with_responses(
        monkeypatch, [http_error(429, "3"), URLError("network secret"), io.BytesIO(b'{"ok": true}')]
    )
    assert client.post("/embeddings/user", {"user_did": "did:plc:a"}) == {"ok": True}
    assert [call.args for call in sleep.call_args_list] == [(3.0,), (2.0,)]
    assert opener.open.call_count == 3
    assert all(call.kwargs["timeout"] == 60 for call in opener.open.call_args_list)
    request = opener.open.call_args.args[0]
    assert request.get_header("X-api-key") == "private-key"


def test_client_retries_three_times_and_does_not_leak_response_body(monkeypatch):
    client, opener, sleep = client_with_responses(monkeypatch, [http_error(502) for _ in range(3)])
    with pytest.raises(average.RequestError, match="^http_502$"):
        client.post("/embeddings/user", {})
    assert opener.open.call_count == 3
    assert [call.args for call in sleep.call_args_list] == [(1.0,), (2.0,)]


def test_incomplete_response_is_a_retryable_transport_failure(monkeypatch):
    class BrokenResponse(io.BytesIO):
        def read(self, *args):
            raise IncompleteRead(b'{"embedding":')

    client, opener, sleep = client_with_responses(
        monkeypatch, [BrokenResponse(), io.BytesIO(b'{"ok": true}')]
    )
    assert client.post("/embeddings/user", {}) == {"ok": True}
    assert opener.open.call_count == 2
    sleep.assert_called_once_with(1.0)


@pytest.mark.parametrize(
    "status,code",
    [(401, None), (403, None), (404, None), (302, None), (502, "inference_not_configured")],
)
def test_client_configuration_errors_are_fatal_without_retry(monkeypatch, status, code):
    client, opener, sleep = client_with_responses(monkeypatch, [http_error(status, code=code)])
    with pytest.raises(average.RunError) as error:
        client.post("/embeddings/user", {})
    assert "NEVER-PUBLISH" not in str(error.value)
    assert opener.open.call_count == 1
    sleep.assert_not_called()


def test_retry_after_date_and_invalid_value(monkeypatch):
    monkeypatch.setattr(average.time, "time", lambda: 0)
    assert average.retry_delay("Thu, 01 Jan 1970 00:00:07 GMT", 0) == 7
    assert average.retry_delay("invalid", 1) == 2
    assert average.retry_delay("-1", 0) == 1


@pytest.mark.parametrize("delay", ["61", "86400", "Thu, 01 Jan 2099 00:00:00 GMT"])
def test_long_retry_after_fails_without_sleep(monkeypatch, delay):
    client, opener, sleep = client_with_responses(monkeypatch, [http_error(429, delay)])
    with pytest.raises(average.RequestError, match="retry_after_too_long"):
        client.post("/embeddings/user", {})
    sleep.assert_not_called()
    assert opener.open.call_count == 1


def test_http_get_and_stable_request_id_across_attempts(monkeypatch):
    client, opener, _ = client_with_responses(
        monkeypatch, [http_error(502), io.BytesIO(b'{"cluster_uuid":"abc"}')]
    )
    assert client.get("/") == {"cluster_uuid": "abc"}
    requests = [call.args[0] for call in opener.open.call_args_list]
    assert requests[0].method == "GET"
    assert requests[0].data is None
    assert requests[0].get_header("X-request-id") == requests[1].get_header("X-request-id")
    assert len(requests[0].get_header("X-request-id")) == 32


@pytest.mark.parametrize(
    "vector", [[], [float("nan")], [float("inf")], [10**1000], [True], ["1"], [[1]]]
)
def test_invalid_vectors_are_rejected(vector):
    result = average.fetch_embedding(
        FakeClient(lambda *_: embedding("did:plc:a", vector)), "did:plc:a", SOURCE
    )
    assert result["status"] == "failed"
    assert result["reason"] == "invalid_embedding_response"
    assert "embedding" not in result


@pytest.mark.parametrize("reason", ["no_likes", "no_embedded_history"])
def test_missing_history_is_explicitly_skipped(reason):
    result = average.fetch_embedding(
        FakeClient(lambda *_: skipped("did:plc:a", reason)), "did:plc:a", SOURCE
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
    result = average.fetch_embedding(FakeClient(lambda *_: response), "did:plc:a", SOURCE)
    assert result["status"] == "failed"
    assert "embedding" not in result


def average_users(client, users, workers=2):
    return average.average_embeddings(
        client, [record["user_did"] for record in users], workers, SOURCE
    )


def test_mean_is_equal_weight_unnormalized_and_returns_only_aggregate_metadata():
    responses = {
        "did:plc:a": embedding("did:plc:a", [2, 4]),
        "did:plc:b": embedding("did:plc:b", [6, 8]),
    }
    client = FakeClient(lambda _, payload: responses[payload["user_did"]])
    result = average_users(client, [user("did:plc:a", 50, 5), user("did:plc:b", 500, 50)])
    assert result["embedding"] == [4, 6]
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


def test_missing_history_updates_aggregate_counts_and_logs_without_retaining_dids(caplog):
    caplog.set_level(logging.INFO, logger=average.__name__)
    responses = {
        "did:plc:a": embedding("did:plc:a", [2, 4]),
        "did:plc:b": skipped("did:plc:b", "no_likes"),
        "did:plc:c": skipped("did:plc:c", "no_embedded_history"),
    }
    client = FakeClient(lambda _, payload: responses[payload["user_did"]])
    result = average_users(client, [user(did) for did in responses])
    assert result["embedding"] == [2, 4]
    assert result["contributing_users"] == 1
    assert result["skipped_users"] == 2
    assert "summary eligible=3 contributing=1 skipped=2 failed=0" in caplog.text
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
        ({**embedding("did:plc:b"), "es_cluster_uuid": "wrong-cluster"}, "source mismatch"),
        ({**skipped("did:plc:b"), "likes_index": "other-index"}, "source mismatch"),
    ],
)
def test_metadata_mismatch_is_global_failure(second, match):
    client = FakeClient(
        lambda _, payload: embedding("did:plc:a") if payload["user_did"] == "did:plc:a" else second
    )
    with pytest.raises(average.RunError, match=match):
        average_users(client, [user("did:plc:a"), user("did:plc:b")])


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


def test_single_failed_user_prevents_partial_average_but_finishes_all_users(caplog):
    caplog.set_level(logging.INFO, logger=average.__name__)

    def respond(_, payload):
        if payload["user_did"] == "did:plc:b":
            raise average.RequestError("upstream_timeout")
        return embedding(payload["user_did"])

    client = FakeClient(respond)
    with pytest.raises(average.RunError, match="1 embedding requests failed"):
        average_users(client, [user(f"did:plc:{letter}") for letter in "abc"])
    assert len(client.requests) == 3
    assert "summary eligible=3 contributing=2 skipped=0 failed=1" in caplog.text
    assert "failed reason=upstream_timeout count=1" in caplog.text
    assert "did:plc:" not in caplog.text


def test_global_failure_stops_submissions_and_drains_active_requests():
    barrier = threading.Barrier(4)

    def respond(*_):
        barrier.wait(timeout=5)
        raise average.RunError("global configuration failure")

    client = FakeClient(respond)
    with pytest.raises(average.RunError, match="configuration"):
        average_users(client, [user(f"did:plc:u{i}") for i in range(1000)], workers=4)
    assert len(client.requests) == 4


def install_pipeline_fakes(monkeypatch, responder=None, collection_failure=False):
    for name in ("POSTHOG_PERSONAL_API_KEY", "GE_ELASTICSEARCH_API_KEY", "GE_API_KEY"):
        monkeypatch.setenv(name, "SECRET-" + name)
    clients = []

    class PipelineClient:
        def __init__(self, service, base_url, headers, insecure=False):
            self.service, self.headers, self.insecure = service, headers, insecure
            self.requests = []
            clients.append(self)

        def get(self, path):
            assert self.service == "Elasticsearch" and path == "/"
            return {"cluster_uuid": SOURCE["es_cluster_uuid"]}

        def post(self, path, payload):
            self.requests.append((path, payload))
            if self.service == "PostHog":
                if collection_failure:
                    raise average.RequestError("http_502")
                return {
                    "results": []
                    if payload["query"]["values"]["after_did"]
                    else [
                        [f"did:plc:{letter}", 50 + number] for number, letter in enumerate("abcdef")
                    ]
                }
            if self.service == "Elasticsearch":
                return es_response({f"did:plc:{letter}": 5 for letter in "abcde"})
            did = payload["user_did"]
            if responder is not None:
                return responder(did)
            return (
                skipped(did)
                if did == "did:plc:c"
                else embedding(did, [2, 4] if did == "did:plc:a" else [6, 8])
            )

    monkeypatch.setattr(average, "JsonClient", PipelineClient)
    return clients


def read_summary(capsys):
    captured = capsys.readouterr()
    assert "SECRET-" not in captured.out + captured.err
    return json.loads(captured.out)


@pytest.mark.parametrize(
    "tls_args,insecure", [([], True), (["--es-insecure"], True), (["--no-es-insecure"], False)]
)
def test_full_pipeline_publishes_only_compact_artifact_and_no_secrets(
    tmp_path, monkeypatch, capsys, tls_args, insecure
):
    clients = install_pipeline_fakes(monkeypatch)
    assert average.main(["--output-dir", str(tmp_path / "output"), *tls_args]) == 0
    summary = read_summary(capsys)
    artifact_path = Path(summary["artifact_path"])
    artifact = json.loads(artifact_path.read_text())
    assert average.validate_artifact(artifact) == artifact
    assert artifact["embedding"] == [5, 7]
    assert artifact_path.name == f"average_user_embedding_{artifact['run_id']}.json"
    assert artifact["run_id"] == summary["run_id"]
    assert artifact["cohort"]["min_likes"] == 5
    assert summary["status"] == "success"
    assert set(summary) == {"status", "artifact_path", "publication", "run_id"}
    assert summary["publication"] is None
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
    assert [client.insecure for client in clients] == [False, insecure, False]
    assert clients[0].headers == {"Authorization": "Bearer SECRET-POSTHOG_PERSONAL_API_KEY"}
    assert clients[1].headers == {"Authorization": "ApiKey SECRET-GE_ELASTICSEARCH_API_KEY"}
    assert clients[2].headers == {"X-API-Key": "SECRET-GE_API_KEY"}
    assert Counter(payload["user_did"] for _, payload in clients[2].requests) == {
        f"did:plc:{letter}": 1 for letter in "abcde"
    }
    assert list((tmp_path / "output").iterdir()) == [artifact_path]
    for path in (tmp_path / "output").iterdir():
        assert "SECRET-" not in path.read_text()
    assert not list((tmp_path / "output").glob(".average*"))


@pytest.mark.parametrize(
    "failure", ["zero", "auth", "mixed", "request", "malformed", "collection", "interrupt"]
)
def test_failed_generation_returns_error_without_output_or_upload(
    tmp_path, monkeypatch, capsys, caplog, failure
):
    caplog.set_level(logging.INFO, logger=average.__name__)

    def respond(did):
        if failure == "zero":
            return skipped(did)
        if failure == "auth":
            raise average.RunError("Embedding API: HTTP 401")
        if failure == "interrupt":
            raise KeyboardInterrupt
        if failure == "request":
            raise average.RequestError("upstream_timeout")
        if failure == "malformed":
            return embedding(did, [float("nan")])
        return embedding(
            did, model=USER_MODEL if did == "did:plc:a" else "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        )

    install_pipeline_fakes(monkeypatch, respond, collection_failure=failure == "collection")
    publish = Mock(side_effect=AssertionError("must not publish"))
    monkeypatch.setattr(average, "publish_artifact", publish)
    assert (
        average.main(
            ["--output-dir", str(tmp_path), "--gcs-output-prefix", "gs://test-bucket/averages"]
        )
        == 1
    )
    summary = read_summary(capsys)
    assert summary["artifact_path"] is None
    assert summary["status"] == "failed" and summary["error"]
    assert summary["publication"] is None
    assert set(summary) == {"status", "artifact_path", "publication", "error"}
    assert not list(tmp_path.iterdir())
    assert "did:" not in caplog.text
    assert "SECRET-" not in caplog.text
    publish.assert_not_called()


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


def test_publication_is_atomic_and_cleans_failed_write(tmp_path, monkeypatch):
    target = tmp_path / "artifact.json"

    def fail_publish(source, destination):
        assert json.loads(source.read_text()) == {"embedding": [1, 2]}
        assert not destination.exists()
        raise OSError("simulated failure")

    monkeypatch.setattr(average.os, "replace", fail_publish)
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
        average, "JsonClient", Mock(side_effect=AssertionError("network must not start"))
    )
    assert average.main(["--output-dir", str(tmp_path)]) == 1
    summary = read_summary(capsys)
    assert "POSTHOG_PERSONAL_API_KEY" in summary["error"]
    assert summary["artifact_path"] is None
    assert summary["status"] == "failed"
    assert not list(tmp_path.iterdir())


def test_embedding_and_retry_logs_are_aggregate_and_safe(monkeypatch, caplog):
    caplog.set_level(logging.INFO, logger=average.__name__)
    vector = [123456.789, -987654.321]
    response = embedding("did:plc:a", vector)
    client, opener, sleep = client_with_responses(
        monkeypatch,
        [
            http_error(504, "7", code="upstream_timeout"),
            io.BytesIO(json.dumps(response).encode()),
        ],
    )
    result = average_users(client, [user("did:plc:a", interactions=75, likes=17)])
    assert result["embedding"] == vector
    assert opener.open.call_count == 2
    sleep.assert_called_once_with(7.0)
    assert client.retry_count == 1
    assert result["contributing_users"] == 1
    assert result["skipped_users"] == 0
    assert USER_MODEL in caplog.text and POST_MODEL in caplog.text
    assert "dimension=2" in caplog.text
    assert "summary eligible=1 contributing=1" in caplog.text
    assert "retries=1" in caplog.text
    assert "did:plc:" not in caplog.text
    assert "history_embedding_count=" not in caplog.text
    assert "request_id=" not in caplog.text
    assert "attempt 1/3" not in caplog.text
    assert "private-key" not in caplog.text and "NEVER-PUBLISH" not in caplog.text
    assert all(str(value) not in caplog.text for value in vector)


def artifact_fixture():
    return json.loads(
        (Path(__file__).parent / "fixtures/average_user_embedding_v2.json").read_text()
    )


@pytest.mark.parametrize(
    "mutate",
    [
        lambda a: a.update(format_version=1),
        lambda a: a.update(format_version=True),
        lambda a: a.update(contributors=[{"user_did": "did:plc:private"}]),
        lambda a: a["cohort"].update(user_dids=["did:plc:private"]),
        lambda a: a["history_policy"].update(private="secret"),
        lambda a: a.update(embedding=[0, 0]),
        lambda a: a.update(embedding=[float("nan"), 1]),
        lambda a: a.update(embedding=[True, 1]),
        lambda a: a.update(dimension=3),
        lambda a: a.update(user_model_uuid="invalid"),
        lambda a: a.update(user_model_uuid=USER_MODEL.upper()),
        lambda a: a.update(run_id="../../other.json"),
        lambda a: a.update(run_id="20261321T163045.123456Z_a1b2c3d4"),
        lambda a: a.update(contributing_users=0),
        lambda a: a["cohort"].update(eligible_users=999),
        lambda a: a["cohort"].update(cutoff="2099-01-01T00:00:00Z"),
        lambda a: a.update(source_completed_at="2000-01-01T00:00:00Z"),
    ],
)
def test_artifact_validation_rejects_malformed_or_private_data(mutate):
    artifact = artifact_fixture()
    mutate(artifact)
    with pytest.raises(average.RunError):
        average.validate_artifact(artifact)


def test_json_duplicate_keys_are_rejected(tmp_path):
    path = tmp_path / "invalid.json"
    path.write_text('{"format_version":2,"format_version":1}')
    with pytest.raises(average.RunError, match="duplicate JSON keys"):
        average.load_artifact(path)


def test_defaults_are_generic_and_local_only():
    args = average.build_parser().parse_args([])
    assert args.es_url == "https://localhost:9200"
    assert args.es_insecure is True
    assert args.api_url == "http://localhost:8300"
    assert args.output_dir == "./outputs/average_user_embeddings/"
    assert args.gcs_output_prefix is None


def setup_gcs(monkeypatch, *, existing=None, upload_failure=None):
    from google.api_core.exceptions import PreconditionFailed
    from google.cloud import storage

    blob, pinned, bucket, client = Mock(), Mock(), Mock(), Mock()
    blob.generation = 123
    pinned.download_as_bytes.return_value = existing
    if existing is not None:
        blob.upload_from_string.side_effect = PreconditionFailed("SECRET BODY")
    elif upload_failure:
        blob.upload_from_string.side_effect = upload_failure
    bucket.blob.side_effect = lambda name, generation=None: pinned if generation else blob
    client.bucket.return_value = bucket
    monkeypatch.setattr(storage, "Client", lambda: client)
    return client, blob, pinned


def artifact_file(tmp_path):
    path = tmp_path / "input.json"
    path.write_text(json.dumps(artifact_fixture(), separators=(",", ":")))
    return path


def test_gcs_upload_uses_exact_bytes_immutable_name_and_bounded_retries(tmp_path, monkeypatch):
    path = artifact_file(tmp_path)
    client, blob, _ = setup_gcs(monkeypatch)
    result = average.publish_artifact(path, "gs://test-bucket/averages/")
    artifact = artifact_fixture()
    expected = (
        f"averages/{POST_MODEL}/{USER_MODEL}/average_user_embedding_{artifact['run_id']}.json"
    )
    assert result["uri"] == f"gs://test-bucket/{expected}"
    assert result["generation"] == "123"
    assert result["sha256"] == average.hashlib.sha256(path.read_bytes()).hexdigest()
    client.bucket.assert_called_once_with("test-bucket")
    client.bucket.return_value.blob.assert_called_once_with(expected)
    args, kwargs = blob.upload_from_string.call_args
    assert args == (path.read_bytes(),)
    assert kwargs["if_generation_match"] == 0 and kwargs["timeout"] == 60
    assert kwargs["retry"].deadline == 180


def test_gcs_existing_identical_generation_is_idempotent(tmp_path, monkeypatch):
    path = artifact_file(tmp_path)
    client, blob, pinned = setup_gcs(monkeypatch, existing=path.read_bytes())
    assert average.publish_artifact(path, "gs://test-bucket/averages")["generation"] == "123"
    blob.reload.assert_called_once()
    assert client.bucket.return_value.blob.call_args.kwargs == {"generation": 123}
    assert pinned.download_as_bytes.call_args.kwargs["if_generation_match"] == 123
    assert pinned.download_as_bytes.call_args.kwargs["timeout"] == 60


def test_gcs_existing_different_bytes_refuses_overwrite(tmp_path, monkeypatch):
    path = artifact_file(tmp_path)
    _, blob, _ = setup_gcs(monkeypatch, existing=b"different")
    with pytest.raises(average.RunError, match="different bytes"):
        average.publish_artifact(path, "gs://test-bucket/averages")
    assert blob.upload_from_string.call_count == 1


def test_upload_failure_preserves_only_local_artifact(tmp_path, monkeypatch, capsys):
    install_pipeline_fakes(monkeypatch)
    setup_gcs(monkeypatch, upload_failure=RuntimeError("SECRET-RAW-ERROR"))
    assert (
        average.main(
            ["--output-dir", str(tmp_path), "--gcs-output-prefix", "gs://test-bucket/averages"]
        )
        == 1
    )
    summary = read_summary(capsys)
    artifact_path = Path(summary["artifact_path"])
    assert list(tmp_path.iterdir()) == [artifact_path]
    assert json.loads(artifact_path.read_text())["run_id"] == summary["run_id"]
    assert summary["publication"] is None
    assert summary["status"] == "failed"
    assert summary["error"]
    for path in tmp_path.iterdir():
        assert "SECRET-RAW-ERROR" not in path.read_text()


def test_publish_only_needs_no_collection_credentials_and_preserves_original_id(
    tmp_path, monkeypatch, capsys
):
    path = artifact_file(tmp_path)
    for name in ("POSTHOG_PERSONAL_API_KEY", "GE_ELASTICSEARCH_API_KEY", "GE_API_KEY"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(average, "JsonClient", Mock(side_effect=AssertionError("no collection")))
    monkeypatch.chdir(tmp_path)
    _, blob, _ = setup_gcs(monkeypatch)
    assert (
        average.main(
            [
                "--publish-artifact",
                str(path),
                "--gcs-output-prefix",
                "gs://test-bucket/averages",
            ]
        )
        == 0
    )
    summary = read_summary(capsys)
    assert artifact_fixture()["run_id"] in summary["publication"]["uri"]
    assert summary["run_id"] == artifact_fixture()["run_id"]
    assert blob.upload_from_string.call_args.args == (path.read_bytes(),)
    assert summary["artifact_path"] == str(path)
    assert set(summary) == {"status", "artifact_path", "publication", "run_id"}
    assert list(tmp_path.iterdir()) == [path]


def test_publish_only_requires_explicit_destination(tmp_path, capsys):
    path = artifact_file(tmp_path)
    with pytest.raises(SystemExit) as error:
        average.main(["--publish-artifact", str(path)])
    assert error.value.code == 2
    assert "required: --gcs-output-prefix" in capsys.readouterr().err
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize(
    "prefix",
    [
        "https://bucket/path",
        "gs://user:secret@bucket/path",
        "gs://bucket/path?secret=a",
        "gs://bucket/../x",
        "gs://bucket/path#secret",
    ],
)
def test_gcs_prefix_cannot_contain_credentials_or_traversal(prefix):
    with pytest.raises(SystemExit):
        average.build_parser().parse_args(["--gcs-output-prefix", prefix])


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
        average.collect_like_counts(FakeClient(lambda *_: result), "likes", ["did:plc:a"])


@pytest.mark.parametrize("interrupt", ["wait", "request"])
def test_interrupt_stops_submissions_and_drains_active_requests(monkeypatch, interrupt):
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


@pytest.mark.parametrize(
    "flag,value",
    [
        ("--posthog-project-id", "509275"),
        ("--posthog-host", "https://us.posthog.com"),
        ("--min-interaction-seen", "50"),
        ("--es-url", "https://localhost:9200"),
        ("--likes-index", "likes"),
        ("--min-likes", "5"),
        ("--api-url", "http://localhost:8300"),
        ("--workers", "4"),
        ("--output-dir", "ignored"),
        ("--es-insecure", None),
        ("--no-es-insecure", None),
        ("--min-likes=5", None),
    ],
)
def test_publish_only_rejects_explicit_generation_options(tmp_path, capsys, flag, value):
    arguments = [
        "--publish-artifact",
        "irrelevant.json",
        "--gcs-output-prefix",
        "gs://test-bucket/averages",
        flag,
    ]
    if value is not None:
        arguments.append(value)
    with pytest.raises(SystemExit) as error:
        average.main(arguments)
    assert error.value.code == 2
    message = capsys.readouterr().err
    assert "unrecognized arguments" in message
    assert flag in message
    assert not list(tmp_path.iterdir())
