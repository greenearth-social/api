"""Scoped service access to another user's feed preferences."""

from typing import Annotated

from fastapi import APIRouter, Depends, Path, Request

from ..lib.user_preferences import get_preferences_for_user, patch_preferences_for_user
from ..models_feed_transparency import FeedPreferences, PreferencesResponse
from ..security import require_api_scope, scope_responses

router = APIRouter(
    prefix="/users",
    tags=["user-preferences"],
    dependencies=[Depends(require_api_scope("user-config"))],
    responses=scope_responses("user-config"),
)

TargetUserDid = Annotated[
    str,
    Path(
        min_length=7,
        max_length=2048,
        pattern=r"^did:[a-z0-9]+:[A-Za-z0-9._:%-]+$",
        description="Full DID of the user whose preferences are being accessed",
    ),
]


@router.get(
    "/{user_did}/feeds/preferences",
    response_model=PreferencesResponse,
    response_model_exclude_none=True,
)
async def get_user_preferences(request: Request, user_did: TargetUserDid) -> PreferencesResponse:
    """Read the target user's public feed controls, or defaults for a missing user.

    Requires an API key granted the `user-config` scope.
    """
    return await get_preferences_for_user(request.app.state.firestore, user_did)


@router.patch(
    "/{user_did}/feeds/preferences/{feed_name}",
    response_model=FeedPreferences,
    response_model_exclude_none=True,
    responses={404: {"description": "Unknown feed, or a feed without user controls"}},
)
async def patch_user_preferences(
    request: Request,
    user_did: TargetUserDid,
    feed_name: str,
    body: FeedPreferences,
) -> FeedPreferences:
    """Update the target user's controls with all the side effects of a user save.

    Requires an API key granted the `user-config` scope.
    """
    return await patch_preferences_for_user(request.app.state.firestore, user_did, feed_name, body)
