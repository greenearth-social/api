"""atproto DID syntax validation (https://atproto.com/specs/did)."""

from __future__ import annotations

import re

MAX_DID_LENGTH = 2048
_DID_RE = re.compile(r"did:[a-z]+:[a-zA-Z0-9._:%-]*[a-zA-Z0-9._-]")


def is_valid_did(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) <= MAX_DID_LENGTH
        and _DID_RE.fullmatch(value) is not None
    )
