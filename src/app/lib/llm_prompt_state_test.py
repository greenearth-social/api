"""Prompt activation, legacy fallback and transaction conflicts without network access."""

from copy import deepcopy
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest
from google.api_core.exceptions import Aborted

from ..documents import SourceWeightsDocument
from .llm_prompt_state import (
    PromptChangedError,
    get_current_llm_query_vector,
    get_llm_prompt_revision,
    remove_llm_prompt,
    save_fitted_llm_prompt,
    without_prompt_weight,
)

USER = "did:plc:prompt-user"
USER_PATH = "users/prompt-user"
VECTORS = f"{USER_PATH}/llm_query_vectors"
ACCEPTED = f"{USER_PATH}/accepted_feed_slates/your-feed"
NOW = datetime(2026, 10, 25, tzinfo=UTC)
DEFAULTS = {
    "following": 0.3,
    "network_likes": 0.2,
    "authors_topics": 0.25,
    "popular": 0.25,
    "llm": 0,
}


def merge(target, patch):
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            merge(target[key], value)
        else:
            target[key] = deepcopy(value)


class PromptStore:
    """Stage writes and exercise the real Firestore transaction retry wrapper."""

    def __init__(self):
        self.documents: dict[str, dict] = {}
        self.pending: list[tuple[str, dict | None, bool]] = []
        self.sequence = 0
        self.conflict_on_commit = False
        self.db = MagicMock()
        self.db.collection.side_effect = self.collection
        self.transaction = MagicMock()
        self.db.transaction.return_value = self.transaction
        self.transaction._read_only = False
        self.transaction._max_attempts = 5
        self.transaction._id = b"prompt-transaction"
        self.transaction._begin = AsyncMock()
        self.transaction._clean_up.side_effect = self.pending.clear
        self.transaction._rollback = AsyncMock(side_effect=self.pending.clear)
        self.transaction._commit = AsyncMock(side_effect=self.commit)
        self.transaction.set.side_effect = self.set
        self.transaction.delete.side_effect = lambda ref: self.pending.append(
            (ref.path, None, False)
        )

    def snapshot(self, path):
        snapshot = MagicMock()
        snapshot.id = path.rsplit("/", 1)[1]
        snapshot.exists = path in self.documents
        snapshot.to_dict.return_value = deepcopy(self.documents.get(path))
        return snapshot

    def collection(self, path):
        collection = MagicMock()

        def document(key=None):
            self.sequence += 1
            return self.reference(f"{path}/{key or f'auto-{self.sequence}'}")

        collection.document.side_effect = document
        collection.order_by.return_value = collection
        collection.limit.return_value = collection

        async def stream(*, transaction=None):
            assert not self.pending, "Firestore disallows reads after writes"
            rows = [key for key in self.documents if key.startswith(f"{path}/")]
            rows.sort(key=lambda key: self.documents[key]["updated_at"], reverse=True)
            for key in rows[:1]:
                yield self.snapshot(key)

        collection.stream.side_effect = stream
        return collection

    def reference(self, path):
        ref = MagicMock()
        ref.path = path
        ref.id = path.rsplit("/", 1)[1]
        ref.collection.side_effect = lambda name: self.collection(f"{path}/{name}")

        async def get(*, transaction=None):
            assert not self.pending, "Firestore disallows reads after writes"
            return self.snapshot(path)

        ref.get = AsyncMock(side_effect=get)
        return ref

    def set(self, ref, data, merge=False):
        self.pending.append((ref.path, deepcopy(data), merge))

    async def commit(self):
        if self.conflict_on_commit:
            self.conflict_on_commit = False
            self.documents[USER_PATH]["llm_prompt_state"] = {
                "active_prompt_key": "another-tab",
                "revision": 2,
            }
            raise Aborted("concurrent prompt operation")
        for path, data, should_merge in self.pending:
            if data is None:
                self.documents.pop(path, None)
            elif should_merge:
                merge(self.documents.setdefault(path, {}), data)
            else:
                self.documents[path] = data
        self.pending.clear()

    def seed(self, *, explicit=False, llm=0.2):
        weights = {key: value * (1 - llm) for key, value in DEFAULTS.items()}
        weights["llm"] = llm
        self.documents[USER_PATH] = {
            "user_did": USER,
            "feed_preferences": {
                "your-feed": {"source_weights": weights, "freshness": 3},
                "best-of-friends": {"freshness": 2},
            },
        }
        if explicit:
            self.documents[USER_PATH]["llm_prompt_state"] = {
                "active_prompt_key": "latest",
                "revision": 1,
            }
        for index, key in enumerate(["older", "latest"]):
            self.documents[f"{VECTORS}/{key}"] = {
                "prompt_key": key,
                "user_did": USER,
                "query_vector": [0.1],
                "prompt": key,
                "created_at": NOW + timedelta(seconds=index),
                "updated_at": NOW + timedelta(seconds=index),
            }
        self.documents[ACCEPTED] = {"request_id": "old-preview"}


@pytest.mark.asyncio
async def test_legacy_read_uses_latest_until_activation_state_exists():
    store = PromptStore()
    store.seed()
    current = await get_current_llm_query_vector(store.db, USER)
    assert current is not None and current.prompt_key == "latest"
    assert await get_llm_prompt_revision(store.db, USER) == 0
    store.documents[USER_PATH]["llm_prompt_state"] = {"active_prompt_key": "older", "revision": 1}
    current = await get_current_llm_query_vector(store.db, USER)
    assert current is not None and current.prompt_key == "older"


@pytest.mark.asyncio
@pytest.mark.parametrize("active_key", [None, "missing-vector"])
async def test_explicit_state_never_reactivates_history(active_key):
    store = PromptStore()
    store.seed()
    store.documents[USER_PATH]["llm_prompt_state"] = {
        "active_prompt_key": active_key,
        "revision": 3,
    }
    assert await get_current_llm_query_vector(store.db, USER) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("explicit", [False, True])
async def test_remove_is_atomic_persistent_and_idempotent(explicit):
    store = PromptStore()
    store.seed(explicit=explicit)
    updated = await remove_llm_prompt(store.db, USER, "latest")
    assert updated.source_weights is not None
    assert updated.source_weights.model_dump() == DEFAULTS
    assert updated.freshness == 3
    saved = store.documents[USER_PATH]
    assert saved["feed_preferences"]["best-of-friends"] == {"freshness": 2}
    assert saved["llm_prompt_state"] == {
        "active_prompt_key": None,
        "revision": 2 if explicit else 1,
    }
    assert ACCEPTED not in store.documents
    assert len([key for key in store.documents if key.startswith(f"{VECTORS}/")]) == 2
    assert await get_current_llm_query_vector(store.db, USER) is None
    again = await remove_llm_prompt(store.db, USER, "latest")
    assert again == updated
    assert saved["llm_prompt_state"]["revision"] == (2 if explicit else 1)


@pytest.mark.asyncio
async def test_wrong_key_does_not_change_prompt_preferences_or_preview():
    store = PromptStore()
    store.seed(explicit=True)
    original = deepcopy(store.documents)
    with pytest.raises(PromptChangedError):
        await remove_llm_prompt(store.db, USER, "older")
    assert store.documents == original


@pytest.mark.asyncio
async def test_a_fit_started_before_removal_cannot_reactivate_it():
    store = PromptStore()
    store.seed()
    revision = await get_llm_prompt_revision(store.db, USER)
    await remove_llm_prompt(store.db, USER, "latest")
    with pytest.raises(PromptChangedError):
        await save_fitted_llm_prompt(store.db, USER, [0.2], "late fit", expected_revision=revision)
    assert await get_current_llm_query_vector(store.db, USER) is None
    assert len([key for key in store.documents if key.startswith(f"{VECTORS}/")]) == 2
    current_revision = await get_llm_prompt_revision(store.db, USER)
    created = await save_fitted_llm_prompt(
        store.db, USER, [0.3], "new fit", expected_revision=current_revision
    )
    assert await get_current_llm_query_vector(store.db, USER) == created
    assert store.documents[USER_PATH]["llm_prompt_state"]["revision"] == current_revision + 1


@pytest.mark.asyncio
async def test_transaction_retry_rechecks_revision_before_storing_a_fit():
    store = PromptStore()
    store.seed(explicit=True)
    store.conflict_on_commit = True
    with pytest.raises(PromptChangedError):
        await save_fitted_llm_prompt(store.db, USER, [0.2], "raced fit", expected_revision=1)
    assert store.documents[USER_PATH]["llm_prompt_state"] == {
        "active_prompt_key": "another-tab",
        "revision": 2,
    }
    assert len([key for key in store.documents if key.startswith(f"{VECTORS}/")]) == 2


@pytest.mark.asyncio
async def test_remove_retry_cannot_clear_a_prompt_created_in_another_tab():
    store = PromptStore()
    store.seed(explicit=True)
    store.conflict_on_commit = True
    with pytest.raises(PromptChangedError):
        await remove_llm_prompt(store.db, USER, "latest")
    assert store.documents[USER_PATH]["llm_prompt_state"]["active_prompt_key"] == "another-tab"
    assert ACCEPTED in store.documents


@pytest.mark.asyncio
async def test_first_fit_atomically_creates_activation_and_the_vector():
    store = PromptStore()
    record = await save_fitted_llm_prompt(store.db, USER, [0.3], "first", expected_revision=0)
    assert store.documents[USER_PATH]["llm_prompt_state"] == {
        "active_prompt_key": record.prompt_key,
        "revision": 1,
    }
    assert await get_current_llm_query_vector(store.db, USER) == record


@pytest.mark.parametrize(
    "weights,expected",
    [
        (
            {"following": 0, "network_likes": 0, "authors_topics": 0, "popular": 0, "llm": 1},
            DEFAULTS,
        ),
        (
            {
                "following": 0.1,
                "network_likes": 0.2,
                "authors_topics": 0.3,
                "popular": 0.1,
                "llm": 0.3,
            },
            {
                "following": 0.14,
                "network_likes": 0.29,
                "authors_topics": 0.43,
                "popular": 0.14,
                "llm": 0,
            },
        ),
        (
            {
                "following": 0.333333,
                "network_likes": 0,
                "authors_topics": 0.333333,
                "popular": 0.333334,
                "llm": 0,
            },
            {
                "following": 0.333333,
                "network_likes": 0,
                "authors_topics": 0.333333,
                "popular": 0.333334,
                "llm": 0,
            },
        ),
    ],
)
def test_weight_redistribution(weights, expected):
    result = without_prompt_weight(SourceWeightsDocument.model_validate(weights))
    assert result.model_dump() == expected
