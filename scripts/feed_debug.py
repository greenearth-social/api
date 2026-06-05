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

Reads Firestore connection from the same env vars as the API server:
    GE_FIRESTORE_PROJECT, GE_FIRESTORE_DATABASE, GE_FIRESTORE_EMULATOR_HOST
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from app.documents import FeedDebugDocument
from app.lib.firestore import (
    get_feed_debug,
    get_recent_feed_debug,
    get_user_by_username,
    init_firestore_client,
    set_user_debug_flag,
)


async def _resolve_user_did(db, user: str) -> str | None:
    """Resolve a handle or DID argument to a user DID."""
    if user.startswith("did:"):
        return user
    doc = await get_user_by_username(db, user)
    return doc.user_did if doc else None


async def cmd_enable(user: str, enabled: bool) -> None:
    db = init_firestore_client()
    user_did = await _resolve_user_did(db, user)
    if user_did is None:
        print(f"No user found for '{user}'.", file=sys.stderr)
        sys.exit(1)
    try:
        await set_user_debug_flag(db, user_did, enabled)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)
    state = "enabled" if enabled else "disabled"
    print(f"Feed debugging {state} for {user_did}.")


async def cmd_list(user: str, limit: int) -> None:
    db = init_firestore_client()
    user_did = await _resolve_user_did(db, user)
    if user_did is None:
        print(f"No user found for '{user}'.", file=sys.stderr)
        sys.exit(1)
    docs = await get_recent_feed_debug(db, user_did, limit=limit)
    if not docs:
        print(f"No feed-debug records for {user_did}.")
        return
    print(f"{'request_id':<34} {'feed':<16} {'items':<6} {'generated_at'}")
    print("-" * 80)
    for d in docs:
        generated = d.generated_at.strftime("%Y-%m-%dT%H:%M:%SZ")
        print(f"{d.request_id:<34} {d.feed_name:<16} {len(d.final_order):<6} {generated}")


def _media_summary(c) -> str:
    parts = []
    if c.image_count:
        parts.append(f"img:{c.image_count}")
    elif c.contains_images:
        parts.append("img")
    if c.video_count:
        parts.append(f"vid:{c.video_count}")
    elif c.contains_video:
        parts.append("vid")
    if c.external_uri:
        parts.append("link")
    return ",".join(parts) if parts else "-"


def _print_show(doc: FeedDebugDocument) -> None:
    print(f"request_id  : {doc.request_id}")
    print(f"user        : {doc.username or doc.user_did}")
    print(f"feed        : {doc.feed_name}")
    print(f"generated_at: {doc.generated_at.strftime('%Y-%m-%dT%H:%M:%SZ')}")
    print(f"regenerated : {doc.regenerated}")
    print(f"ranker      : {doc.ranker_model or '(none)'}    diversify: {doc.diversify}")

    req = doc.generate_request
    gen_specs = ", ".join(f"{g.name}({g.weight})" for g in req.generators)
    print()
    print("--- generation request ---")
    print(f"generators  : {gen_specs}")
    print(f"infill      : {req.infill or '(none)'}")
    print(f"num_candidates: {req.num_candidates}    video_only: {req.video_only}")
    print(f"excluded    : {len(req.exclude_uris)} uris")

    if doc.user_features:
        print()
        print("--- user features ---")
        for uf in doc.user_features:
            print(
                f"{uf.source}: {len(uf.liked_post_uris)} liked posts, "
                f"{uf.num_embeddings} with embeddings"
            )

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

    # Candidate metadata: prefer the final (sanitized) candidate, else first seen.
    meta: dict[str, object] = {}
    for result in doc.generator_outputs:
        for c in result.candidates:
            if c.at_uri:
                meta.setdefault(c.at_uri, c)
    for c in doc.final_candidates:
        if c.at_uri:
            meta[c.at_uri] = c

    print()
    print(f"--- final feed ({len(doc.final_order)} items) ---")
    for pos, uri in enumerate(doc.final_order):
        _print_item(uri, pos, generators_by_uri, rank_by_uri, after_rank_pos, meta)

    discarded = sorted(set(generators_by_uri) - set(final_pos))
    if discarded:
        print()
        print(f"--- discarded ({len(discarded)} candidates not in final feed) ---")
        for uri in discarded:
            gens = generators_by_uri.get(uri, [])
            gen_str = ", ".join(f"{n}:{_fmt_score(s)}" for n, s in gens)
            print(f"  {uri}  [{gen_str}]")


def _fmt_score(score: float | None) -> str:
    return f"{score:.4f}" if score is not None else "-"


def _print_item(uri, pos, generators_by_uri, rank_by_uri, after_rank_pos, meta) -> None:
    c = meta.get(uri)
    gens = generators_by_uri.get(uri, [])
    gen_str = ", ".join(f"{n}:{_fmt_score(s)}" for n, s in gens) or "(infill/unknown)"
    rank, rank_score = rank_by_uri.get(uri, (None, None))
    ar = after_rank_pos.get(uri)
    author = getattr(c, "author_username", None) or getattr(c, "author_did", None) or "?"
    media = _media_summary(c) if c is not None else "-"
    content = (getattr(c, "content", None) or "").replace("\n", " ")

    print(f"[{pos}] {uri}")
    print(f"     author: {author}   media: {media}")
    print(
        f"     generators: {gen_str}   rank: {rank} (score {_fmt_score(rank_score)})"
        f"   after_rank_pos: {ar}"
    )
    if content:
        print(f"     content: {content}")


async def cmd_show(user: str, request_id: str) -> None:
    db = init_firestore_client()
    user_did = await _resolve_user_did(db, user)
    if user_did is None:
        print(f"No user found for '{user}'.", file=sys.stderr)
        sys.exit(1)
    doc = await get_feed_debug(db, user_did, request_id)
    if doc is None:
        print(f"No feed-debug record {request_id} for {user_did}.", file=sys.stderr)
        sys.exit(1)
    _print_show(doc)


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

    args = parser.parse_args()

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
