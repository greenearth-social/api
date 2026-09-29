from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, field_validator

from ..lib.did import is_valid_did
from ..lib.oauth_revoke import revoke_oauth_grant
from ..security import RequireAdminOrUser

router = APIRouter(tags=["oauth"])


class RevokeRequest(BaseModel):
    did: str

    @field_validator("did")
    @classmethod
    def _well_formed(cls, value: str) -> str:
        if not is_valid_did(value):
            raise ValueError("did is not a well-formed DID")
        return value


@router.post("/api/oauth/revoke")
async def revoke_oauth(request: Request, body: RevokeRequest, caller: RequireAdminOrUser) -> JSONResponse:
    """Revoke the stored OAuth grant for ``did`` at its authorization server (no data deleted).

    Callable by an admin API key, or by the user whose DID it is.
    """
    if caller.kind == "user" and caller.identity != body.did:
        raise HTTPException(status_code=403, detail="Users may only revoke their own grant")
    db = getattr(request.app.state, "firestore", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Firestore unavailable")
    outcome = await revoke_oauth_grant(db, body.did, caller.actor)
    return JSONResponse(
        {"did": body.did, "outcome": outcome}, status_code=502 if outcome == "failed" else 200
    )
