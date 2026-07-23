#!/usr/bin/env python3
"""Terminal Bluesky client for viewing a locally-generated feed (issue #285).

Requests ``getFeedSkeleton`` from a running api as a chosen dev persona,
hydrates each post from Elasticsearch, and prints the feed. Nothing is
published and no write path is exercised — it just lets a developer see the
feed they are working on.

Hydration is deliberately local-only: the skeleton returns bare AT URIs, and
we resolve them against the environment's own Elasticsearch rather than the
public AppView, so this works entirely against seeded fixture data with no
network access and no Bluesky credentials.

Pipeline detail (per-item rank, ranker score, which generator retrieved a
post) comes from the feed snapshot the api writes for every feed load. The
``feedContext`` token on each skeleton item carries the snapshot's request id
in its payload, so we read the id straight out of it. Pass ``--no-pipeline``
to skip that lookup and show the feed exactly as a real client would see it.

Usually invoked through the dev environment, which supplies the persona,
endpoints, and secrets:

    devctl feed
    devctl feed your-feed --user did:plc:... --limit 50

Run it directly against a devenv api with the same environment the api
container has:

    pipenv run python scripts/feed_view.py your-feed --user did:plc:...
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import binascii
import json
import os
import sys
from datetime import datetime
from typing import Any, NoReturn

from rich.console import Console
from rich.text import Text

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from feed_format import fmt_score, media_badges, relative_time

console = Console()

# The flagship feed: the full retrieve -> rank -> diversify path, which is
# what someone running this usually wants to look at.
DEFAULT_FEED = "your-feed"
DEFAULT_FEED_PUBLISHER = "did:web:test"
POSTS_ALIAS = "posts_recent"

# Fields worth pulling for display. Post embeddings live in the same documents
# and are large enough that fetching them makes paging visibly slow.
_SOURCE_FIELDS = [
    "at_uri",
    "author_did",
    "content",
    "created_at",
    "like_count",
    "contains_images",
    "image_count",
    "contains_video",
    "video_count",
    "external_embed",
    "quote_post",
]


def _die(message: str) -> NoReturn:
    console.print(f"[red]{message}[/red]")
    sys.exit(1)


def feed_uri(feed: str, publisher: str) -> str:
    """Full AT URI for a feed, passing through one that is already a URI."""
    if feed.startswith("at://"):
        return feed
    return f"at://{publisher}/app.bsky.feed.generator/{feed}"


def request_id_from_feed_context(feed_context: str | None) -> str | None:
    """Pull the snapshot request id out of a ``feedContext`` token.

    The token is ``<base64url payload>.<signature>``; the payload is plain JSON
    carrying ``rid``. We only read it, never trust it — the id is used to look
    up a local snapshot for display, so a malformed or unsigned token costs
    nothing beyond losing the pipeline detail.
    """
    if not feed_context:
        return None
    payload = feed_context.split(".", 1)[0]
    padded = payload + "=" * (-len(payload) % 4)
    try:
        decoded = json.loads(base64.urlsafe_b64decode(padded))
    except (ValueError, binascii.Error):
        return None
    rid = decoded.get("rid") if isinstance(decoded, dict) else None
    return rid if isinstance(rid, str) else None


async def fetch_skeleton(
    *,
    base_url: str,
    uri: str,
    user_did: str,
    secret: str,
    limit: int,
    cursor: str | None,
) -> dict[str, Any]:
    """Call getFeedSkeleton as *user_did* via the dev-session header."""
    import httpx

    params: dict[str, Any] = {"feed": uri, "limit": limit}
    if cursor:
        params["cursor"] = cursor
    headers = {"X-Dev-Session": secret, "X-Dev-Session-DID": user_did}
    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.get(
            f"{base_url}/xrpc/app.bsky.feed.getFeedSkeleton", params=params, headers=headers
        )
    if resp.status_code != 200:
        _die(f"api returned {resp.status_code}: {resp.text[:400]}")
    return resp.json()


async def hydrate_posts(es, uris: list[str]) -> dict[str, dict[str, Any]]:
    """Fetch post documents for *uris*, keyed by AT URI.

    Uses an ``ids`` query rather than mget: posts are indexed with
    ``routing=author_did``, and a direct id lookup would need that routing
    value, which the skeleton doesn't give us.
    """
    if not uris:
        return {}
    resp = await es.search(
        index=POSTS_ALIAS,
        query={"ids": {"values": uris}},
        _source=_SOURCE_FIELDS,
        size=len(uris),
    )
    return {hit["_source"]["at_uri"]: hit["_source"] for hit in resp["hits"]["hits"]}


async def load_pipeline_meta(user_did: str, request_id: str) -> dict[str, Any] | None:
    """Per-URI pipeline metadata from the feed snapshot, or None if unavailable.

    Best-effort: the snapshot is written in a background task, so a very fast
    caller can beat it, and an environment without Firestore configured has no
    snapshots at all. Either way the feed still renders.
    """
    try:
        from app.lib.firestore import get_feed_snapshot, init_firestore_client

        db = init_firestore_client()
        doc = await get_feed_snapshot(db, user_did, request_id)
    except Exception:
        return None
    if doc is None:
        return None
    return {
        "ranker_model": doc.ranker_model,
        "diversify": doc.diversify,
        "items": {m.at_uri: m for m in doc.items_meta},
    }


def _parse_created_at(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _author_label(did: str | None) -> str:
    """Display label for a post author.

    Elasticsearch stores only ``author_did`` — handles live in the AppView,
    which this tool deliberately doesn't call — so the DID is what we can show.
    """
    return did or "unknown author"


def render_post(
    *,
    position: int,
    uri: str,
    source: dict[str, Any] | None,
    meta: Any | None,
) -> Text:
    """One post as it appears in the feed listing."""
    out = Text()
    out.append(f"{position:>3}. ", style="bold green")

    if source is None:
        out.append("(not in Elasticsearch) ", style="red")
        out.append(uri, style="dim cyan")
        return out

    out.append(_author_label(source.get("author_did")), style="bold white")

    created = _parse_created_at(source.get("created_at"))
    if created is not None:
        out.append(f"  {relative_time(created)}", style="dim cyan")

    likes = source.get("like_count")
    if likes:
        out.append(f"  ♥ {likes}", style="magenta")

    badges = media_badges(
        image_count=source.get("image_count"),
        contains_images=bool(source.get("contains_images")),
        video_count=source.get("video_count"),
        contains_video=bool(source.get("contains_video")),
        external_uri=source.get("external_embed"),
    )
    if badges is not None:
        out.append("  ")
        out.append_text(badges)

    content = (source.get("content") or "").strip()
    if content:
        for line in content.splitlines():
            out.append("\n     ")
            out.append(line, style="default")
    else:
        out.append("\n     (no text)", style="dim")

    if meta is not None:
        pipeline = Text("\n     ", style="dim")
        gens = ", ".join(f"{g.name} ({fmt_score(g.score)})" for g in meta.generators)
        pipeline.append("via ", style="dim")
        pipeline.append(gens or "infill/unknown", style="cyan")
        if meta.rank is not None:
            pipeline.append("  ranked ", style="dim")
            pipeline.append(f"#{meta.rank}", style="yellow")
        if meta.rank_score is not None:
            pipeline.append(f" ({fmt_score(meta.rank_score)})", style="dim")
        out.append_text(pipeline)

    out.append(f"\n     {uri}", style="dim cyan")
    return out


def render_page(
    *,
    feed: str,
    user_did: str,
    skeleton: dict[str, Any],
    hydrated: dict[str, dict[str, Any]],
    pipeline: dict[str, Any] | None,
    start_position: int,
) -> None:
    items = skeleton.get("feed", [])
    header = Text()
    header.append(feed, style="bold magenta")
    header.append(f"  as {user_did}", style="dim")
    header.append(f"  {len(items)} item{'s' if len(items) != 1 else ''}", style="dim")
    if pipeline and pipeline.get("ranker_model"):
        header.append(f"  ranker {pipeline['ranker_model']}", style="dim")
    console.print()
    console.print(header)
    console.print()

    if not items:
        console.print("[yellow]Feed is empty.[/yellow]")
        return

    meta_by_uri = pipeline["items"] if pipeline else {}
    missing = 0
    for offset, item in enumerate(items):
        uri = item.get("post", "")
        source = hydrated.get(uri)
        if source is None:
            missing += 1
        console.print(
            render_post(
                position=start_position + offset,
                uri=uri,
                source=source,
                meta=meta_by_uri.get(uri),
            )
        )
        console.print()

    if missing:
        console.print(
            f"[yellow]{missing} post{'s' if missing != 1 else ''} could not be hydrated — "
            f"the feed references URIs that aren't in this environment's index.[/yellow]"
        )


async def run(args: argparse.Namespace) -> None:
    secret = os.environ.get("GE_DEV_SESSION_SECRET")
    if not secret:
        _die(
            "GE_DEV_SESSION_SECRET is not set. This tool talks to the api as a dev "
            "session; run it through `devctl feed`, or export the same secret the "
            "api container uses."
        )

    user_did = args.user or os.environ.get("GE_PROBE_USER_DID", "")
    if not user_did:
        _die("No persona given. Pass --user did:plc:... (or seed the environment first).")
    if not user_did.startswith("did:plc:"):
        _die(f"--user must be a did:plc DID, got: {user_did}")

    uri = feed_uri(args.feed, args.publisher)

    es_key = os.environ.get("GE_ELASTICSEARCH_API_KEY")
    if not es_key:
        _die("GE_ELASTICSEARCH_API_KEY is not set — needed to hydrate posts.")

    from elasticsearch import AsyncElasticsearch

    es = AsyncElasticsearch(
        hosts=[os.environ.get("GE_ELASTICSEARCH_URL", "http://localhost:9200")],
        api_key=es_key,
        verify_certs=False,
        request_timeout=30,
    )

    try:
        cursor = args.cursor
        position = 1
        for page in range(args.pages):
            skeleton = await fetch_skeleton(
                base_url=args.api_url,
                uri=uri,
                user_did=user_did,
                secret=secret,
                limit=args.limit,
                cursor=cursor,
            )

            if args.json:
                print(json.dumps(skeleton, indent=2))
            else:
                items = skeleton.get("feed", [])
                uris = [i["post"] for i in items if i.get("post")]
                hydrated = await hydrate_posts(es, uris)

                pipeline = None
                if not args.no_pipeline and items:
                    rid = request_id_from_feed_context(items[0].get("feedContext"))
                    if rid:
                        pipeline = await load_pipeline_meta(user_did, rid)

                render_page(
                    feed=args.feed,
                    user_did=user_did,
                    skeleton=skeleton,
                    hydrated=hydrated,
                    pipeline=pipeline,
                    start_position=position,
                )
                position += len(items)

            cursor = skeleton.get("cursor")
            if not cursor or not skeleton.get("feed"):
                break
            if page + 1 < args.pages:
                console.print("[dim]— next page —[/dim]")

        if cursor and not args.json:
            console.print(f"[dim]next cursor: {cursor}[/dim]")
            console.print("[dim]continue with: --cursor <cursor>  (or --pages N)[/dim]")
    finally:
        await es.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="View a locally-generated Green Earth feed in the terminal"
    )
    parser.add_argument(
        "feed",
        nargs="?",
        default=DEFAULT_FEED,
        help=f"Feed name or full at:// URI (default {DEFAULT_FEED})",
    )
    parser.add_argument("--user", help="Persona DID (defaults to the seeded GE_PROBE_USER_DID)")
    parser.add_argument("--limit", type=int, default=20, help="Posts per page (default 20)")
    parser.add_argument("--cursor", help="Resume from a cursor returned by an earlier run")
    parser.add_argument(
        "--pages", type=int, default=1, help="Fetch this many pages, following cursors (default 1)"
    )
    parser.add_argument(
        "--no-pipeline",
        action="store_true",
        help="Skip the snapshot lookup and show the feed as a plain client would",
    )
    parser.add_argument(
        "--json", action="store_true", help="Print the raw getFeedSkeleton response instead"
    )
    parser.add_argument(
        "--api-url",
        default=os.environ.get("GE_DEV_API_URL", "http://localhost:8000"),
        help="Base URL of the api (default http://localhost:8000)",
    )
    parser.add_argument(
        "--publisher",
        default=os.environ.get("GE_DEV_FEED_PUBLISHER", DEFAULT_FEED_PUBLISHER),
        help=f"Feed publisher DID (default {DEFAULT_FEED_PUBLISHER})",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if args.pages < 1:
        _die("--pages must be at least 1")
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
