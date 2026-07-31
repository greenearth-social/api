import os
import pytest
from fastapi.testclient import TestClient

from ..main import app
from ..lib.embeddings import MINILM_L12_EMBEDDING_FIELD, encode_float32_b64


@pytest.fixture
def es_response():
    return {
        "hits": {
            "hits": [
                {
                    "_score": 1.5,
                    "_source": {
                        "at_uri": "at://1",
                        "content": "hello world",
                        "contains_video": True,
                    },
                    "fields": {MINILM_L12_EMBEDDING_FIELD: [[0.1, 0.2]]},
                }
            ]
        }
    }


@pytest.fixture(autouse=True)
def fake_app_es(es_response):
    class FakeEs:
            async def search(
                self, *, index=None, query=None, size=None,
                _source=None, docvalue_fields=None, **kwargs,
            ):
                return es_response

    # ensure a predictable API key for tests and restore previous value
    prev = os.environ.get("API_KEY")
    os.environ["API_KEY"] = "testkey"

    from ..main import app

    app.state.es = FakeEs()
    yield
    try:
        delattr(app.state, "es")
    except Exception:
        pass
    if prev is None:
        del os.environ["API_KEY"]
    else:
        os.environ["API_KEY"] = prev


def test_search_returns_embedding():
    client = TestClient(app, headers={"X-API-Key": "testkey"})
    resp = client.get("/skylight/search?q=hello")
    assert resp.status_code == 200

    expected = encode_float32_b64([0.1, 0.2])
    assert resp.json() == {
        "results": [
            {
                "at_uri": "at://1",
                "content": "hello world",
                "minilm_l12_embedding": expected,
                "score": 1.5,
                "generator_name": None,
                "author_did": None,
                "author_username": None,
                "contains_images": None,
                "contains_video": None,
                "image_count": None,
                "video_count": None,
                "external_uri": None,
                "like_count": None,
            }
        ]
    }
