import pytest
from fastapi.testclient import TestClient

from ..main import app


@pytest.fixture
def es_response():
    return {
        "hits": {
            "hits": [
                {
                    "_source": {
                        "at_uri": "at://1",
                        "content": "hello world",
                        "contains_video": True,
                        "embeddings": {
                            "all_MiniLM_L12_v2": [0.1, 0.2],
                            "all_MiniLM_L6_v2": [0.3, 0.4],
                        },
                    }
                }
            ]
        }
    }


@pytest.fixture(autouse=True)
def fake_app_es(es_response):
    class FakeEs:
            async def search(self, *, index=None, query=None, size=None, **kwargs):
                # If this is a lookup by at_uri terms, return a doc. Allow
                # simulating a 'missing' at_uri that has no embeddings.
                if isinstance(query, dict) and "terms" in query:
                    at_list = query.get("terms", {}).get("at_uri")
                    # If the test asks for an at_uri named "missing", return
                    # a document without embeddings to trigger a 404 path.
                    if isinstance(at_list, (list, tuple)) and "missing" in at_list:
                        doc = {**es_response["hits"]["hits"][0]["_source"], "at_uri": "at://missing", "embeddings": {}}
                        return {"hits": {"hits": [{"_source": doc}]}}
                    return {"hits": {"hits": [{"_source": {**es_response["hits"]["hits"][0]["_source"], "at_uri": "at://1"}}]}}
                # If it's a knn search (similar), return same hit list
                if isinstance(query, dict) and "knn" in query:
                    return es_response
                return es_response

    from ..main import app

    app.state.es = FakeEs()
    yield
    try:
        delattr(app.state, "es")
    except Exception:
        pass


def test_search_returns_embedding():
    client = TestClient(app)
    resp = client.get("/skylight/search?q=hello")
    assert resp.status_code == 200
    from .skylight import encode_float32_b64

    expected = encode_float32_b64([0.1, 0.2])
    assert resp.json() == {
        "results": [
            {
                "at_uri": "at://1",
                "content": "hello world",
                "minilm_l12_embedding": expected,
            }
        ]
    }


def test_similar_with_at_uris():
    client = TestClient(app)
    resp = client.post("/skylight/similar", json={"at_uris": ["at://1"], "size": 1})
    assert resp.status_code == 200
    from .skylight import encode_float32_b64

    expected = encode_float32_b64([0.1, 0.2])
    assert resp.json() == {
        "results": [
            {
                "at_uri": "at://1",
                "content": "hello world",
                "minilm_l12_embedding": expected,
            }
        ]
    }


def test_similar_with_embeddings():
    client = TestClient(app)
    from .skylight import encode_float32_b64

    b64 = encode_float32_b64([0.1, 0.2])
    resp = client.post("/skylight/similar", json={"embeddings": [b64], "size": 1})
    assert resp.status_code == 200
    expected = b64
    assert resp.json() == {
        "results": [
            {
                "at_uri": "at://1",
                "content": "hello world",
                "minilm_l12_embedding": expected,
            }
        ]
    }


def test_similar_no_embeddings_for_at_uris_returns_404():
    client = TestClient(app)
    resp = client.post("/skylight/similar", json={"at_uris": ["missing"], "size": 1})
    assert resp.status_code == 404


def test_similar_invalid_base64_returns_400():
    client = TestClient(app)
    resp = client.post("/skylight/similar", json={"embeddings": ["not-base64"], "size": 1})
    assert resp.status_code == 400


def test_similar_embedding_dimension_mismatch_returns_400():
    client = TestClient(app)
    from .skylight import encode_float32_b64

    b1 = encode_float32_b64([0.1, 0.2])
    b2 = encode_float32_b64([0.1])
    resp = client.post("/skylight/similar", json={"embeddings": [b1, b2], "size": 1})
    assert resp.status_code == 400
