import pytest

from .did import is_valid_did


@pytest.mark.parametrize(
    "value",
    ["did:plc:abc123xyz", "did:web:example.com", "did:web:example.com%3A8080", "did:plc:a.b_c-d"],
)
def test_valid_dids(value):
    assert is_valid_did(value)


@pytest.mark.parametrize(
    "value",
    [
        "",
        "did:plc:",
        "did:plc",
        "not-a-did",
        "did:plc:abc/def",
        "did:plc:abc def",
        "did:plc:abc\n",
        "did:PLC:abc",
        "did:plc:abc:",
        "did:plc:" + "a" * 2100,
        None,
        123,
    ],
)
def test_invalid_dids(value):
    assert not is_valid_did(value)
