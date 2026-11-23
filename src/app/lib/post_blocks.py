"""Live Bluesky author relationships for post display."""

import asyncio
from typing import Literal

import httpx

from .http_client import get_http_client

_RELATIONSHIPS_URL = "https://api.bsky.app/xrpc/app.bsky.graph.getRelationships"
_BATCH_SIZE = 30
_ATTEMPT_TIMEOUT_SECONDS = 2.4
_RETRY_DELAY_SECONDS = 0.1
_BLOCK_FIELDS = ("blocking", "blockedBy", "blockingByList", "blockedByList")

AuthorBlockStatus = Literal["clear", "blocked", "unavailable"]


def _parse_relationships(
    data: object, viewer_did: str, requested: set[str]
) -> dict[str, AuthorBlockStatus]:
    if not isinstance(data, dict) or data.get("actor") != viewer_did:
        return {}
    entries = data.get("relationships")
    if not isinstance(entries, list):
        return {}

    statuses: dict[str, AuthorBlockStatus] = {}
    seen: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        not_found = entry.get("notFound") is True
        did = entry.get("actor" if not_found else "did")
        if not isinstance(did, str) or did not in requested:
            continue
        if did in seen:
            statuses.pop(did, None)
            continue
        seen.add(did)
        if not_found:
            statuses[did] = "unavailable"
            continue
        fields = [entry[name] for name in _BLOCK_FIELDS if name in entry]
        if all(isinstance(value, str) and value.startswith("at://") for value in fields):
            statuses[did] = "blocked" if fields else "clear"
    return statuses


async def _fetch_batch(
    client: httpx.AsyncClient, viewer_did: str, author_dids: list[str]
) -> dict[str, AuthorBlockStatus]:
    statuses: dict[str, AuthorBlockStatus] = {}
    pending = author_dids
    for attempt in range(2):
        try:
            async with asyncio.timeout(_ATTEMPT_TIMEOUT_SECONDS):
                response = await client.get(
                    _RELATIONSHIPS_URL,
                    params={"actor": viewer_did, "others": pending},
                    timeout=_ATTEMPT_TIMEOUT_SECONDS,
                )
                response.raise_for_status()
                statuses.update(_parse_relationships(response.json(), viewer_did, set(pending)))
        except httpx.HTTPStatusError as error:
            if error.response.status_code != 429 and error.response.status_code < 500:
                break
        except (httpx.RequestError, TimeoutError, ValueError):
            pass
        pending = [did for did in pending if did not in statuses]
        if not pending:
            break
        if attempt == 0:
            await asyncio.sleep(_RETRY_DELAY_SECONDS)
    return statuses


async def get_author_block_statuses(
    viewer_did: str, author_dids: list[str]
) -> dict[str, AuthorBlockStatus]:
    """Return verified statuses, leaving authors with failed lookups out of the result."""
    distinct = list(dict.fromkeys(did for did in author_dids if did != viewer_did))
    statuses: dict[str, AuthorBlockStatus] = {}
    if viewer_did in author_dids:
        statuses[viewer_did] = "clear"
    if distinct:
        client = get_http_client()
        batches = await asyncio.gather(
            *(
                _fetch_batch(client, viewer_did, distinct[start : start + _BATCH_SIZE])
                for start in range(0, len(distinct), _BATCH_SIZE)
            )
        )
        for batch in batches:
            statuses.update(batch)
    return statuses
