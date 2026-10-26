"""Atomic prompt activation/removal, retaining the user's historical vectors."""

from datetime import UTC, datetime

from google.cloud.firestore import AsyncClient, Query, async_transactional

from ..documents import (
    FeedPreferencesDocument,
    LlmPromptStateDocument,
    LlmQueryVectorDocument,
    SourceWeightsDocument,
    UserDocument,
)
from .feed_preferences import DEFAULT_SOURCE_WEIGHTS, resolve_feed_preferences
from .firestore import (
    ACCEPTED_FEED_SLATES_COLLECTION,
    LLM_QUERY_VECTORS_SUBCOLLECTION,
    USERS_COLLECTION,
    get_latest_llm_query_vector,
    get_llm_query_vector,
    user_doc_id,
)


class PromptChangedError(Exception):
    """Another prompt operation superseded this request."""


def _state(data: dict | None) -> LlmPromptStateDocument | None:
    raw = (data or {}).get("llm_prompt_state")
    return LlmPromptStateDocument.model_validate(raw) if raw is not None else None


async def get_llm_prompt_revision(db: AsyncClient, user_did: str) -> int:
    snapshot = await db.collection(USERS_COLLECTION).document(user_doc_id(user_did)).get()
    state = _state(snapshot.to_dict() if snapshot.exists else None)
    return state.revision if state is not None else 0


async def get_current_llm_query_vector(
    db: AsyncClient, user_did: str
) -> LlmQueryVectorDocument | None:
    snapshot = await db.collection(USERS_COLLECTION).document(user_doc_id(user_did)).get()
    state = _state(snapshot.to_dict() if snapshot.exists else None)
    if state is None:
        return await get_latest_llm_query_vector(db, user_did)
    if state.active_prompt_key is None:
        return None
    return await get_llm_query_vector(db, user_did, state.active_prompt_key)


async def save_fitted_llm_prompt(
    db: AsyncClient,
    user_did: str,
    query_vector: list[float],
    prompt: str,
    *,
    expected_revision: int,
) -> LlmQueryVectorDocument:
    user_ref = db.collection(USERS_COLLECTION).document(user_doc_id(user_did))
    vector_ref = user_ref.collection(LLM_QUERY_VECTORS_SUBCOLLECTION).document()
    now = datetime.now(UTC)
    record = LlmQueryVectorDocument(
        prompt_key=vector_ref.id,
        user_did=user_did,
        query_vector=query_vector,
        prompt=prompt,
        created_at=now,
        updated_at=now,
    )

    @async_transactional
    async def _save(transaction) -> LlmQueryVectorDocument:
        snapshot = await user_ref.get(transaction=transaction)
        state = _state(snapshot.to_dict() if snapshot.exists else None)
        revision = state.revision if state is not None else 0
        if revision != expected_revision:
            raise PromptChangedError
        transaction.set(vector_ref, record.model_dump())
        transaction.set(
            user_ref,
            {
                "user_did": user_did,
                "llm_prompt_state": {
                    "active_prompt_key": record.prompt_key,
                    "revision": revision + 1,
                },
                "created_by_load_test": False,
                "updated_at": now,
            },
            merge=True,
        )
        transaction.delete(
            user_ref.collection(ACCEPTED_FEED_SLATES_COLLECTION).document("your-feed")
        )
        return record

    return await _save(db.transaction())


def without_prompt_weight(weights: SourceWeightsDocument) -> SourceWeightsDocument:
    """Redistribute in percentage points, using the frontend's largest-remainder rule."""
    if weights.llm == 0:
        return weights.model_copy()
    values = weights.model_dump(exclude={"llm"})
    total = sum(values.values())
    if total <= 0:
        return DEFAULT_SOURCE_WEIGHTS.model_copy()
    desired = {key: value / total * 100 for key, value in values.items()}
    cents = {key: int(value) for key, value in desired.items()}
    order = sorted(desired, key=lambda key: desired[key] - cents[key], reverse=True)
    for key in order[: 100 - sum(cents.values())]:
        cents[key] += 1
    return SourceWeightsDocument(llm=0, **{key: value / 100 for key, value in cents.items()})


async def remove_llm_prompt(
    db: AsyncClient, user_did: str, prompt_key: str
) -> FeedPreferencesDocument:
    user_ref = db.collection(USERS_COLLECTION).document(user_doc_id(user_did))

    @async_transactional
    async def _remove(transaction) -> FeedPreferencesDocument:
        snapshot = await user_ref.get(transaction=transaction)
        data = snapshot.to_dict() if snapshot.exists else None
        state = _state(data)
        active_key = state.active_prompt_key if state is not None else None
        if state is None:
            # Read the legacy selection in the same transaction, before any writes.
            query = (
                user_ref.collection(LLM_QUERY_VECTORS_SUBCOLLECTION)
                .order_by("updated_at", direction=Query.DESCENDING)
                .limit(1)
            )
            async for doc in query.stream(transaction=transaction):
                active_key = doc.id
        if active_key is not None and active_key != prompt_key:
            raise PromptChangedError

        user = UserDocument.model_validate(data) if data is not None else None
        preferences = resolve_feed_preferences(user, "your-feed")
        assert preferences.source_weights is not None
        updated = preferences.model_copy(
            update={"source_weights": without_prompt_weight(preferences.source_weights)}
        )
        revision = state.revision if state is not None else 0
        # An idempotent retry must not invalidate a fit started after the first removal.
        if active_key is not None or state is None:
            revision += 1
        transaction.set(
            user_ref,
            {
                "user_did": user_did,
                "llm_prompt_state": {"active_prompt_key": None, "revision": revision},
                "feed_preferences": {"your-feed": updated.model_dump(exclude_none=True)},
                "created_by_load_test": False,
                "updated_at": datetime.now(UTC),
            },
            merge=True,
        )
        transaction.delete(
            user_ref.collection(ACCEPTED_FEED_SLATES_COLLECTION).document("your-feed")
        )
        return updated

    return await _remove(db.transaction())
