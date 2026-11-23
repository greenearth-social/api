"""Tests for live Bluesky relationship lookups."""

import httpx
import pytest

from . import post_blocks

VIEWER = "did:plc:viewer"


@pytest.mark.asyncio
async def test_checks_both_directions_and_lists_in_one_request(monkeypatch):
    authors = {
        "did:plc:outgoing": "blocking",
        "did:plc:incoming": "blockedBy",
        "did:plc:outgoing-list": "blockingByList",
        "did:plc:incoming-list": "blockedByList",
    }
    calls = []

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(
            200,
            json={
                "actor": VIEWER,
                "relationships": [
                    {"did": did, field: "at://did:plc:block/app.bsky.graph.block/record"}
                    for did, field in authors.items()
                ]
                + [
                    {"did": "did:plc:clear", "following": "at://did:plc:viewer/follow/one"},
                    {"actor": "did:plc:gone", "notFound": True},
                ],
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        monkeypatch.setattr(post_blocks, "get_http_client", lambda: client)
        statuses = await post_blocks.get_author_block_statuses(
            VIEWER, [*authors, "did:plc:clear", "did:plc:gone", "did:plc:clear", VIEWER]
        )

    assert len(calls) == 1
    assert calls[0].url.host == "api.bsky.app"
    assert calls[0].url.params["actor"] == VIEWER
    assert calls[0].url.params.get_list("others") == [*authors, "did:plc:clear", "did:plc:gone"]
    assert statuses == {
        **dict.fromkeys(authors, "blocked"),
        "did:plc:clear": "clear",
        "did:plc:gone": "unavailable",
        VIEWER: "clear",
    }


@pytest.mark.asyncio
async def test_failed_batch_keeps_successful_batches(monkeypatch):
    authors = [f"did:plc:author{i}" for i in range(32)]
    batches = []

    def respond(request: httpx.Request) -> httpx.Response:
        batch = request.url.params.get_list("others")
        batches.append(batch)
        if authors[-1] in batch:
            return httpx.Response(503)
        return httpx.Response(
            200, json={"actor": VIEWER, "relationships": [{"did": did} for did in batch]}
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        monkeypatch.setattr(post_blocks, "get_http_client", lambda: client)
        statuses = await post_blocks.get_author_block_statuses(VIEWER, authors)

    assert batches.count(authors[:30]) == 1
    assert batches.count(authors[30:]) == 2
    assert statuses == dict.fromkeys(authors[:30], "clear")


@pytest.mark.asyncio
async def test_retries_only_unverified_entries(monkeypatch):
    calls = []
    verified, missing = "did:plc:verified", "did:plc:missing"

    def respond(request: httpx.Request) -> httpx.Response:
        requested = request.url.params.get_list("others")
        calls.append(requested)
        entries = [{"did": verified}] if len(calls) == 1 else [{"did": missing}]
        return httpx.Response(200, json={"actor": VIEWER, "relationships": entries})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        monkeypatch.setattr(post_blocks, "get_http_client", lambda: client)
        statuses = await post_blocks.get_author_block_statuses(VIEWER, [verified, missing])

    assert calls == [[verified, missing], [missing]]
    assert statuses == {verified: "clear", missing: "clear"}
