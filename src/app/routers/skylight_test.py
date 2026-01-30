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
            return es_response

    from ..main import app

    app.state.es = FakeEs()
    yield
    try:
        delattr(app.state, "es")
    except Exception:
        pass


def test_skylight_returns_embedding():
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
