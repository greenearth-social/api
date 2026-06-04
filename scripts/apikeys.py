#!/usr/bin/env python3
"""API key management CLI for Green Earth API.

Run from the api/ directory:
    pipenv run python scripts/apikeys.py generate alice@example.com
    pipenv run python scripts/apikeys.py list
    pipenv run python scripts/apikeys.py revoke a1b2c3d4
    pipenv run python scripts/apikeys.py usage a1b2c3d4

Reads Firestore connection from the same env vars as the API server:
    GE_FIRESTORE_PROJECT, GE_FIRESTORE_DATABASE, GE_FIRESTORE_EMULATOR_HOST
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from app.lib.api_keys import create_api_key, get_api_key_doc, list_api_keys, revoke_api_key
from app.lib.firestore import init_firestore_client


async def cmd_generate(email: str) -> None:
    db = init_firestore_client()
    doc, full_key = await create_api_key(db, email)
    print(f"Key ID : {doc.key_id}")
    print(f"Email  : {doc.email}")
    print(f"Key    : {full_key}")
    print()
    print("IMPORTANT: This is the only time the plaintext key will be shown.")


async def cmd_list() -> None:
    db = init_firestore_client()
    keys = await list_api_keys(db)
    if not keys:
        print("No API keys found.")
        return
    print(f"{'key_id':<10} {'email':<30} {'active':<8} {'calls/mo':<10} {'last_used_at'}")
    print("-" * 80)
    for k in keys:
        last_used = k.last_used_at.strftime("%Y-%m-%dT%H:%M:%SZ")
        print(f"{k.key_id:<10} {k.email:<30} {str(k.is_active):<8} {k.monthly_call_count:<10} {last_used}")


async def cmd_revoke(key_id: str) -> None:
    db = init_firestore_client()
    ok = await revoke_api_key(db, key_id)
    if ok:
        print(f"Revoked key {key_id}.")
    else:
        print(f"Key {key_id} not found.", file=sys.stderr)
        sys.exit(1)


async def cmd_usage(key_id: str) -> None:
    db = init_firestore_client()
    doc = await get_api_key_doc(db, key_id)
    if doc is None:
        print(f"Key {key_id} not found.", file=sys.stderr)
        sys.exit(1)
    last_used = doc.last_used_at.strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"key_id             : {doc.key_id}")
    print(f"email              : {doc.email}")
    print(f"is_active          : {doc.is_active}")
    print(f"last_used_at       : {last_used}")
    print(f"monthly_period     : {doc.monthly_period}")
    print(f"monthly_call_count : {doc.monthly_call_count}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Green Earth API key management")
    sub = parser.add_subparsers(dest="command", required=True)

    gen = sub.add_parser("generate", help="Issue a new API key")
    gen.add_argument("email", help="Owner email address")

    sub.add_parser("list", help="List all API keys")

    rev = sub.add_parser("revoke", help="Deactivate an API key")
    rev.add_argument("key_id", help="8-char key ID")

    usage = sub.add_parser("usage", help="Show usage stats for an API key")
    usage.add_argument("key_id", help="8-char key ID")

    args = parser.parse_args()

    if args.command == "generate":
        asyncio.run(cmd_generate(args.email))
    elif args.command == "list":
        asyncio.run(cmd_list())
    elif args.command == "revoke":
        asyncio.run(cmd_revoke(args.key_id))
    elif args.command == "usage":
        asyncio.run(cmd_usage(args.key_id))


if __name__ == "__main__":
    main()
