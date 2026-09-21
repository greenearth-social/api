"""Exercise the real endpoint-to-producer contract without external services."""

import io
import json
import logging
import math
from collections import Counter
from types import SimpleNamespace
from urllib.error import HTTPError
from urllib.parse import urlsplit

import average_user_embedding as producer
import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.lib import inference, user_history_cache
from app.lib.embeddings import MINILM_L12_EMBEDDING_DIM, MINILM_L12_EMBEDDING_FIELD
from app.routers import embeddings
from app.security import verify_api_key

USER_MODEL = "1affd684bc7f45f895e488f83dd0a2fa"
POST_MODEL = "9b946f280fd84899a7f82246fbc34d17"
OTHER_POST_MODEL = "2" * 32
DIDS = [f"did:plc:{name}" for name in ("a", "b", "c", "d")]
INTERACTIONS = dict(zip(DIDS, (50, 100, 70, 60), strict=True))
LIKE_COUNTS = dict(zip(DIDS, (5, 500, 5, 4), strict=True))


def search_response(hits):
    return {
        "timed_out": False,
        "_shards": {"total": 1, "successful": 1, "failed": 0},
        "hits": {"total": {"value": len(hits), "relation": "eq"}, "hits": hits},
    }


class HistoryES:
    """Reply history must participate; any candidate search fails the test."""

    async def info(self):
        return {"cluster_uuid": "same-cluster"}

    async def search(self, *, index, query, **kwargs):
        if index == "likes":
            did = query["bool"]["filter"][0]["terms"]["author_did"][0]
            return search_response(
                [
                    {
                        "_source": {
                            "subject_uri": f"at://{did}/app.bsky.feed.post/liked",
                            "created_at": "2026-09-20T00:00:00Z",
                        }
                    }
                ]
            )
        assert index in ("posts", "replies"), "Candidate retrieval is forbidden"
        uri = query["terms"]["at_uri"][0]
        did = uri.split("/")[2]
        # a's history is a post; b's is a reply; c has no embedded document.
        if (did, index) not in ((DIDS[0], "posts"), (DIDS[1], "replies")):
            return search_response([])
        marker = 1.0 if did == DIDS[0] else 3.0
        return search_response(
            [
                {
                    "_source": {
                        "at_uri": uri,
                        "content": "history content",
                        "author_did": did,
                        "like_count": 2,
                    },
                    "fields": {
                        MINILM_L12_EMBEDDING_FIELD: [[marker] * MINILM_L12_EMBEDDING_DIM],
                    },
                }
            ]
        )


@pytest.mark.parametrize("model_changes", [False, True])
def test_real_endpoint_to_local_artifact(tmp_path, monkeypatch, caplog, model_changes):
    caplog.set_level(logging.INFO, logger=producer.__name__)
    app = FastAPI()
    app.include_router(embeddings.router)
    app.dependency_overrides[verify_api_key] = lambda: "test-key-id"
    app.state.es = HistoryES()
    monkeypatch.setattr(
        embeddings, "get_inference_settings", lambda: ("https://inference.test", "secret")
    )
    inference_calls = []

    async def predict(url, *, json, headers):
        assert url.endswith("/models/user-tower/predict")
        marker = json["history_embeddings"][0][0]
        inference_calls.append(marker)
        return httpx.Response(
            200,
            json={
                "outputs": [[1, 2, 4] if marker == 1 else [3, 6, 8]],
                "model_type": "user-tower",
                "model_uuid": USER_MODEL,
                "paired_post_model_uuid": (
                    OTHER_POST_MODEL if model_changes and marker == 3 else POST_MODEL
                ),
            },
        )

    monkeypatch.setattr(inference, "get_http_client", lambda: SimpleNamespace(post=predict))

    monkeypatch.setattr(user_history_cache, "get_user_history_cache", lambda: None)
    for variable in ("POSTHOG_PERSONAL_API_KEY", "GE_ELASTICSEARCH_API_KEY", "GE_API_KEY"):
        monkeypatch.setenv(variable, "integration-test-key")

    real_client = producer.JsonClient
    endpoint_calls = []
    with TestClient(app) as endpoint:

        class OfflineTransport:
            def open(self, request, *, timeout):
                assert timeout == 60
                url = urlsplit(request.full_url)
                body = json.loads(request.data) if request.data else None
                if url.hostname == "posthog.test":
                    after = body["query"]["values"]["after_did"]
                    result = {
                        "results": [
                            [did, count] for did, count in INTERACTIONS.items() if did > after
                        ],
                    }
                elif url.hostname == "es.test":
                    if url.path == "/":
                        result = {"cluster_uuid": "same-cluster"}
                    else:
                        assert url.path == "/likes/_search"
                        batch = body["query"]["terms"]["author_did"]
                        result = {
                            **search_response([]),
                            "aggregations": {
                                "users": {
                                    "sum_other_doc_count": 0,
                                    "doc_count_error_upper_bound": 0,
                                    "buckets": [
                                        {"key": did, "doc_count": LIKE_COUNTS[did]} for did in batch
                                    ],
                                }
                            },
                        }
                else:
                    assert url.hostname == "api.test"
                    endpoint_calls.append(body["user_did"])
                    response = endpoint.post(url.path, json=body, headers=dict(request.headers))
                    if response.status_code >= 400:
                        raise HTTPError(
                            request.full_url,
                            response.status_code,
                            "request failed",
                            response.headers,
                            io.BytesIO(response.content),
                        )
                    result = response.json()
                return io.BytesIO(json.dumps(result).encode())

        def make_client(*args, **kwargs):
            client = real_client(*args, **kwargs)
            client.opener = OfflineTransport()
            return client

        monkeypatch.setattr(producer, "JsonClient", make_client)
        args = producer.build_parser().parse_args(
            [
                "--posthog-host",
                "https://posthog.test",
                "--es-url",
                "https://es.test",
                "--api-url",
                "https://api.test",
                "--workers",
                "1",
                "--output-dir",
                str(tmp_path / "results"),
            ]
        )
        summary = producer.run(args)

    assert sorted(inference_calls) == [1.0, 3.0]
    assert all(count == 1 for count in Counter(endpoint_calls).values())
    assert DIDS[3] not in endpoint_calls
    assert not {"report_path", "log_path", "output_dir", "counts"} & summary.keys()
    if model_changes:
        assert summary["status"] == "failed"
        assert summary["artifact_path"] is None
        assert "mixed model" in summary["error"]
        assert "failed=1" in caplog.text
        assert not list((tmp_path / "results").glob("*"))
    else:
        assert summary["status"] == "success"
        artifact_path = tmp_path / "results" / f"average_user_embedding_{summary['run_id']}.json"
        artifact, data = producer.load_artifact(artifact_path)
        assert summary["artifact_path"] == str(artifact_path)
        assert list((tmp_path / "results").iterdir()) == [artifact_path]
        assert artifact["format_version"] == 1
        assert artifact["embedding"] == pytest.approx(
            [2 / math.sqrt(56), 4 / math.sqrt(56), 6 / math.sqrt(56)]
        )
        assert artifact["post_model_uuid"] == POST_MODEL
        assert artifact["user_model_uuid"] == USER_MODEL
        assert artifact["contributing_users"] == 2
        assert {
            key: artifact["cohort"][key]
            for key in ("posthog_users", "below_min_likes", "eligible_users", "skipped_users")
        } == {
            "posthog_users": 4,
            "below_min_likes": 1,
            "eligible_users": 3,
            "skipped_users": 1,
        }
        assert "contributing=2" in caplog.text
        assert "skipped=1" in caplog.text
        assert "failed=0" in caplog.text
        assert "no_embedded_history" in caplog.text
        assert b"did:" not in data
        assert b"integration-test-key" not in data
        assert "contributors" not in artifact
        assert "skipped" not in artifact
