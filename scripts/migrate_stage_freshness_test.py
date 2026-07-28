"""Tests for the stage freshness migration."""

from unittest.mock import AsyncMock, MagicMock

import pytest
from migrate_stage_freshness import (
    NEW_FRESHNESS,
    OLD_FRESHNESS,
    migrate_stage_freshness,
)


def _async_iter(items):
    async def generate():
        for item in items:
            yield item

    return generate()


def _mock_db(document_count: int):
    db = MagicMock()
    query = MagicMock()
    documents = []
    for index in range(document_count):
        document = MagicMock()
        document.reference = f"users/user-{index}"
        documents.append(document)
    query.stream.return_value = _async_iter(documents)
    db.collection.return_value.where.return_value = query
    return db, documents


@pytest.mark.asyncio
async def test_dry_run_counts_matches_without_writing():
    db, _documents = _mock_db(3)
    update_document = AsyncMock()

    result = await migrate_stage_freshness(
        db,
        apply=False,
        update_document=update_document,
    )

    assert result.matched == 3
    assert result.updated == 0
    assert result.skipped == 0
    update_document.assert_not_awaited()
    field_filter = db.collection.return_value.where.call_args.kwargs["filter"]
    assert field_filter.field_path == "freshness"
    assert field_filter.value == OLD_FRESHNESS


@pytest.mark.asyncio
async def test_apply_updates_only_documents_still_at_old_value():
    db, documents = _mock_db(3)
    update_document = AsyncMock(side_effect=[True, False, True])

    result = await migrate_stage_freshness(
        db,
        apply=True,
        update_document=update_document,
    )

    assert result.matched == 3
    assert result.updated == 2
    assert result.skipped == 1
    assert [call.args[1] for call in update_document.await_args_list] == [
        document.reference for document in documents
    ]
    assert NEW_FRESHNESS == 5
