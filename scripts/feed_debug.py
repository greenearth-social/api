#!/usr/bin/env python3
"""Feed-debug inspection CLI for Green Earth API.

Everything happens in the context of one user, so the user is the first
positional argument (a handle like ``alice.bsky.social`` or a ``did:`` DID),
followed by exactly one action flag.

Run from the api/ directory:
    pipenv run python scripts/feed_debug.py alice.bsky.social --enable
    pipenv run python scripts/feed_debug.py alice.bsky.social --disable
    pipenv run python scripts/feed_debug.py alice.bsky.social --list
    pipenv run python scripts/feed_debug.py alice.bsky.social --list --limit 50
    pipenv run python scripts/feed_debug.py alice.bsky.social --show <request_id>

Defaults to the local Firestore emulator (``--environment dev``). Target a
deployed environment with ``--environment stage`` / ``--environment prod``
(requires GCP credentials, e.g. ``gcloud auth application-default login``);
``--env`` is accepted as an alias:
    pipenv run python scripts/feed_debug.py alice.bsky.social --list --environment stage

Reads Firestore connection from the same env vars as the API server:
    GE_FIRESTORE_PROJECT, GE_FIRESTORE_DATABASE, GE_FIRESTORE_EMULATOR_HOST
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import datetime, timezone
from typing import NoReturn

from rich import box
from rich.console import Console, Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from app.documents import FeedDebugDocument
from app.lib.firestore import (
    get_feed_debug,
    get_recent_feed_debug,
    get_user,
    get_user_by_username,
    init_firestore_client,
    set_user_debug_flag,
)

console = Console()

# GCP project + Firestore database per environment. Both environments live in
# the same project and are separated by database (see scripts/gcp_setup.sh).
GCP_PROJECT = "greenearth-471522"
_ENVIRONMENTS = {
    "stage": "greenearth-stage",
    "prod": "greenearth-prod",
}


def _configure_environment(env: str) -> None:
    """Point Firestore at the chosen environment, in-process.

    ``dev`` (the default) leaves the environment untouched so the local
    ``.env`` (Firestore emulator) is used. ``stage``/``prod`` set the project
    and database explicitly and clear any emulator host — done here rather than
    via shell env vars because ``pipenv`` loads ``.env`` over inline vars.
    """
    if env == "dev":
        return
    os.environ["GE_FIRESTORE_PROJECT"] = GCP_PROJECT
    os.environ["GE_FIRESTORE_DATABASE"] = _ENVIRONMENTS[env]
    os.environ.pop("GE_FIRESTORE_EMULATOR_HOST", None)
    os.environ.pop("FIRESTORE_EMULATOR_HOST", None)
    console.print(f"[dim]→ {env} (database {_ENVIRONMENTS[env]})[/dim]")


async def _resolve_user_did(db, user: str) -> str | None:
    """Resolve a handle or DID argument to a user DID."""
    if user.startswith("did:"):
        return user
    doc = await get_user_by_username(db, user)
    return doc.user_did if doc else None


def _die(message: str) -> NoReturn:
    console.print(f"[red]{message}[/red]")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------


def _relative_time(dt: datetime) -> str:
    """Compact relative-time string for a tz-aware datetime."""
    try:
        delta = datetime.now(timezone.utc) - dt
        secs = delta.total_seconds()
        if secs < 60:
            return "just now"
        if secs < 3_600:
            return f"{int(secs / 60)}m ago"
        if secs < 86_400:
            return f"{int(secs / 3_600)}h ago"
        if delta.days < 30:
            return f"{delta.days}d ago"
        return dt.strftime("%b %d, %Y")
    except Exception:
        return str(dt)


def _fmt_score(score: float | None) -> str:
    return f"{score:.4f}" if score is not None else "—"


def _media_summary(c) -> Text | None:
    """Yellow media badges for a candidate, or None when it has no media."""
    parts = []
    if c.image_count:
        parts.append(f"{c.image_count} image{'s' if c.image_count != 1 else ''}")
    elif c.contains_images:
        parts.append("image")
    if c.video_count:
        parts.append(f"{c.video_count} video{'s' if c.video_count != 1 else ''}")
    elif c.contains_video:
        parts.append("video")
    if c.external_uri:
        parts.append("link")
    if not parts:
        return None
    return Text(f"[{', '.join(parts)}]", style="dim yellow")


# ---------------------------------------------------------------------------
# enable / disable
# ---------------------------------------------------------------------------


async def cmd_enable(user: str, enabled: bool) -> None:
    db = init_firestore_client()
    user_did = await _resolve_user_did(db, user)
    if user_did is None:
        _die(f"No user found for '{user}'.")
    try:
        await set_user_debug_flag(db, user_did, enabled)
    except ValueError as exc:
        _die(str(exc))
    state = "enabled" if enabled else "disabled"
    color = "green" if enabled else "yellow"
    console.print(f"Feed debugging [{color}]{state}[/{color}] for [bold]{user_did}[/bold].")


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


async def cmd_list(user: str, limit: int) -> None:
    db = init_firestore_client()
    user_did = await _resolve_user_did(db, user)
    if user_did is None:
        _die(f"No user found for '{user}'.")

    # Surface the current flag state, but still list any existing records.
    user_doc = await get_user(db, user_did)
    if user_doc is None or not user_doc.debug_feeds:
        console.print(
            "[yellow]Feed debugging is not currently enabled for this user; "
            "no new records will be captured (enable with --enable).[/yellow]"
        )

    docs = await get_recent_feed_debug(db, user_did, limit=limit)
    if not docs:
        console.print(f"[yellow]No feed-debug records for {user_did}.[/yellow]")
        return

    table = Table(
        box=box.SIMPLE_HEAVY, title=f"Recent feed loads — {user_did}", title_justify="left"
    )
    table.add_column("request_id", style="cyan", no_wrap=True)
    table.add_column("feed", style="magenta")
    table.add_column("items", justify="right", style="green")
    table.add_column("ranker")
    table.add_column("when", style="dim")
    for d in docs:
        table.add_row(
            d.request_id,
            d.feed_name,
            str(len(d.final_order)),
            d.ranker_model or "—",
            _relative_time(d.generated_at),
        )
    console.print(table)


# ---------------------------------------------------------------------------
# show
# ---------------------------------------------------------------------------


def _header_panel(doc: FeedDebugDocument) -> Panel:
    req = doc.generate_request
    gen_specs = ", ".join(f"{g.name}({g.weight:g})" for g in req.generators)

    body = Text()
    body.append("user        ", style="dim")
    body.append(f"{doc.username or doc.user_did}\n", style="bold white")
    body.append("feed        ", style="dim")
    body.append(f"{doc.feed_name}", style="magenta")
    body.append(f"   {_relative_time(doc.generated_at)}", style="dim cyan")
    if doc.regenerated:
        body.append("   (regenerated)", style="dim yellow")
    body.append("\n")
    body.append("ranker      ", style="dim")
    body.append(f"{doc.ranker_model or '(none)'}", style="white")
    body.append("   diversify=", style="dim")
    body.append(f"{doc.diversify}\n", style="white")
    body.append("generators  ", style="dim")
    body.append(f"{gen_specs}\n", style="white")
    body.append("infill      ", style="dim")
    body.append(f"{req.infill or '(none)'}", style="white")
    body.append("   candidates=", style="dim")
    body.append(f"{req.num_candidates}", style="white")
    if req.video_only:
        body.append("   video_only", style="yellow")
    if req.exclude_uris:
        body.append(f"   excluded={len(req.exclude_uris)}", style="dim")

    if doc.user_features:
        body.append("\n")
        for uf in doc.user_features:
            body.append("\nuser feats  ", style="dim")
            body.append(
                f"{uf.source}: {len(uf.liked_post_uris)} liked posts, "
                f"{uf.num_embeddings} with embeddings",
                style="white",
            )

    return Panel(
        body,
        title=f"[bold]feed debug[/bold]  [dim cyan]{doc.request_id}[/dim cyan]",
        title_align="left",
        box=box.ROUNDED,
        border_style="blue",
        padding=(0, 2),
    )


def _item_panel(
    uri: str,
    pos: int,
    total: int,
    generators_by_uri: dict,
    rank_by_uri: dict,
    after_rank_pos: dict,
    div_by_uri: dict,
    meta: dict,
) -> Panel:
    c = meta.get(uri)

    # --- author / media line ---
    author_line = Text()
    handle = (getattr(c, "author_username", None) if c else None) or (
        getattr(c, "author_did", None) if c else None
    )
    author_line.append(f"@{handle}" if handle else "unknown author", style="bold white")
    media = _media_summary(c) if c is not None else None
    if media is not None:
        author_line.append("  ")
        author_line.append_text(media)

    # --- pipeline journey: retrieval → ranking → diversification ---
    # ``rank`` is the model's 1-based position with a model score; it only
    # exists when the feed has a ranker. ``order_after_rank`` is the order
    # entering diversification — identical to ``rank`` when a ranker ran, so it's
    # only worth showing in the no-ranker case (where it's the generator-score sort).
    gens = generators_by_uri.get(uri, [])
    gen_str = ", ".join(f"{n} (gen {_fmt_score(s)})" for n, s in gens) or "infill/unknown"
    rank, rank_score = rank_by_uri.get(uri, (None, None))
    ar = after_rank_pos.get(uri)

    journey = Text()
    journey.append("retrieved by ", style="dim")
    journey.append(gen_str, style="cyan")
    if rank is not None:
        journey.append("  →  ranked ", style="dim")
        journey.append(f"#{rank}", style="yellow")
        journey.append(f" (model {_fmt_score(rank_score)})", style="dim")
    elif ar is not None:
        journey.append("  →  by score ", style="dim")
        journey.append(f"#{ar}", style="white")
    journey.append("  →  final ", style="dim")
    journey.append(f"#{pos}", style="bold green")

    # --- diversification breakdown (only when diversification ran) ---
    div = div_by_uri.get(uri)
    diversify_line = None
    if div is not None:
        diversify_line = Text()
        diversify_line.append("diversify    rel ", style="dim")
        diversify_line.append(f"{div.relevance:.3f}", style="white")
        diversify_line.append("   −author ", style="dim")
        diversify_line.append(f"{div.author_penalty:.3f}", style="magenta")
        diversify_line.append("   −content ", style="dim")
        diversify_line.append(f"{div.content_penalty:.3f}", style="cyan")
        diversify_line.append("   → score ", style="dim")
        diversify_line.append(f"{div.score:.3f}", style="white")

    # --- content ---
    content = (getattr(c, "content", None) or "").replace("\n", " ") if c else ""

    group_items = [journey]
    if diversify_line is not None:
        group_items.append(diversify_line)
    group_items.append(author_line)
    if content:
        group_items.append(Text(content, style="default"))

    return Panel(
        Group(*group_items),
        title=f"[grey23]{pos}/{total - 1}[/grey23]  [dim cyan]{uri}[/dim cyan]",
        title_align="left",
        box=box.ROUNDED,
        border_style="grey23",
        padding=(0, 2),
    )


def _discarded_table(discarded: list[str], generators_by_uri: dict) -> Table:
    table = Table(
        box=box.SIMPLE,
        title=f"discarded — {len(discarded)} candidates not in final feed",
        title_justify="left",
        title_style="bold yellow",
    )
    table.add_column("uri", style="dim cyan", no_wrap=True)
    table.add_column("generators")
    for uri in discarded:
        gens = generators_by_uri.get(uri, [])
        gen_str = ", ".join(f"{n} ({_fmt_score(s)})" for n, s in gens)
        table.add_row(uri, gen_str)
    return table


def _render_show(doc: FeedDebugDocument) -> None:
    # Assemble the per-item view by joining stage outputs on at_uri.
    generators_by_uri: dict[str, list[tuple[str, float | None]]] = {}
    for result in doc.generator_outputs:
        for c in result.candidates:
            if c.at_uri:
                generators_by_uri.setdefault(c.at_uri, []).append((result.generator_name, c.score))

    rank_by_uri = {
        r.at_uri: (r.rank, r.rank_score) for r in (doc.ranking.rankings if doc.ranking else [])
    }
    after_rank_pos = {uri: i for i, uri in enumerate(doc.order_after_rank)}
    final_pos = {uri: i for i, uri in enumerate(doc.final_order)}
    div_by_uri = {e.at_uri: e for e in doc.diversification}

    # Candidate metadata: prefer the final (sanitized) candidate, else first seen.
    meta: dict[str, object] = {}
    for result in doc.generator_outputs:
        for c in result.candidates:
            if c.at_uri:
                meta.setdefault(c.at_uri, c)
    for c in doc.final_candidates:
        if c.at_uri:
            meta[c.at_uri] = c

    console.print()
    console.print(_header_panel(doc))

    total = len(doc.final_order)
    console.print(f"\n[bold]final feed[/bold] — {total} item{'s' if total != 1 else ''}")
    legend = (
        "ranked # = model rank (pre-diversification); final # = served position"
        if doc.ranking
        else "by score # = generator-score order (pre-diversification); final # = served position"
    )
    console.print(f"[dim]journey: retrieved by generator (gen score) → {legend}[/dim]\n")
    for pos, uri in enumerate(doc.final_order):
        console.print(
            _item_panel(
                uri, pos, total, generators_by_uri, rank_by_uri, after_rank_pos, div_by_uri, meta
            )
        )

    discarded = sorted(set(generators_by_uri) - set(final_pos))
    if discarded:
        console.print()
        console.print(_discarded_table(discarded, generators_by_uri))
    console.print()


async def cmd_show(user: str, request_id: str) -> None:
    db = init_firestore_client()
    user_did = await _resolve_user_did(db, user)
    if user_did is None:
        _die(f"No user found for '{user}'.")
    doc = await get_feed_debug(db, user_did, request_id)
    if doc is None:
        _die(f"No feed-debug record {request_id} for {user_did}.")
    _render_show(doc)


def main() -> None:
    parser = argparse.ArgumentParser(description="Green Earth feed-debug inspection")
    parser.add_argument("user", help="User handle (e.g. alice.bsky.social) or did:...")

    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--enable", action="store_true", help="Enable feed debugging for the user")
    action.add_argument(
        "--disable", action="store_true", help="Disable feed debugging for the user"
    )
    action.add_argument("--list", action="store_true", help="List recent feed loads")
    action.add_argument("--show", metavar="REQUEST_ID", help="Show one feed load in detail")

    parser.add_argument("--limit", type=int, default=20, help="Max rows for --list (default 20)")
    parser.add_argument(
        "--environment",
        "--env",
        dest="environment",
        choices=["dev", "stage", "prod"],
        default="dev",
        help="Target environment: dev uses the local Firestore emulator (default); "
        "stage/prod connect to the corresponding Firestore database",
    )

    args = parser.parse_args()

    _configure_environment(args.environment)

    if args.enable:
        asyncio.run(cmd_enable(args.user, True))
    elif args.disable:
        asyncio.run(cmd_enable(args.user, False))
    elif args.list:
        asyncio.run(cmd_list(args.user, args.limit))
    elif args.show:
        asyncio.run(cmd_show(args.user, args.show))


if __name__ == "__main__":
    main()
