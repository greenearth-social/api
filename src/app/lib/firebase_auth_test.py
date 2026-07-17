"""Tests for Firebase auth token verification."""

from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException, status

from .firebase_auth import init_firebase_auth, verify_firebase_auth


def _cred():
    return MagicMock()


@pytest.mark.asyncio
async def test_verify_firebase_auth_returns_user_doc_id():
    with patch("app.lib.firebase_auth.auth") as mock_auth:
        mock_auth.verify_id_token.return_value = {"uid": "did:plc:abc123"}

        result = await verify_firebase_auth(_cred())
        assert result == "abc123"


@pytest.mark.asyncio
async def test_verify_firebase_auth_strips_did_plc_prefix():
    with patch("app.lib.firebase_auth.auth") as mock_auth:
        mock_auth.verify_id_token.return_value = {"uid": "did:plc:xyz789"}

        result = await verify_firebase_auth(_cred())
        assert result == "xyz789"


@pytest.mark.asyncio
async def test_verify_firebase_auth_missing_header_raises_401():
    with pytest.raises(HTTPException) as exc_info:
        await verify_firebase_auth(None)

    assert exc_info.value.status_code == status.HTTP_401_UNAUTHORIZED
    assert "Missing" in exc_info.value.detail


@pytest.mark.asyncio
async def test_verify_firebase_auth_invalid_token_raises_401():
    with patch("app.lib.firebase_auth.auth") as mock_auth:
        mock_auth.verify_id_token.side_effect = ValueError("bad token")

        with pytest.raises(HTTPException) as exc_info:
            await verify_firebase_auth(_cred())

    assert exc_info.value.status_code == status.HTTP_401_UNAUTHORIZED
    assert "Invalid" in exc_info.value.detail


@pytest.mark.asyncio
async def test_verify_firebase_auth_token_missing_uid_raises_401():
    with patch("app.lib.firebase_auth.auth") as mock_auth:
        mock_auth.verify_id_token.return_value = {}

        with pytest.raises(HTTPException) as exc_info:
            await verify_firebase_auth(_cred())

    assert exc_info.value.status_code == status.HTTP_401_UNAUTHORIZED


@pytest.mark.asyncio
async def test_verify_firebase_auth_non_did_uid_raises_401():
    with patch("app.lib.firebase_auth.auth") as mock_auth:
        mock_auth.verify_id_token.return_value = {"uid": "not-a-did"}

        with pytest.raises(HTTPException) as exc_info:
            await verify_firebase_auth(_cred())

    assert exc_info.value.status_code == status.HTTP_401_UNAUTHORIZED


def test_init_firebase_auth_is_idempotent():
    """Calling init_firebase_auth twice should not raise."""
    mock_fb = MagicMock()
    mock_fb._apps = False

    with patch("app.lib.firebase_auth.firebase_admin", mock_fb):
        init_firebase_auth()
        assert mock_fb.initialize_app.called

        # Second call: _apps is now truthy → no-op
        mock_fb._apps = True
        mock_fb.initialize_app.reset_mock()
        init_firebase_auth()
        assert not mock_fb.initialize_app.called
