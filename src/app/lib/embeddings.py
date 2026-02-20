"""Shared embedding utilities (encoding, decoding, averaging).

These helpers are used by candidate generators and API routers.
"""

import base64
import struct


def encode_float32_b64(vec: list[float]) -> str:
    """Encode a list of floats as little-endian float32 bytes, then base64.

    Uses ``struct.pack`` with little-endian ``<f`` format for portability.
    """
    if vec is None:
        raise TypeError("vec must not be None")
    if not isinstance(vec, (list, tuple)):
        raise TypeError("vec must be a list or tuple of floats")
    packed = struct.pack(f"<{len(vec)}f", *vec)
    return base64.b64encode(packed).decode("ascii")


def decode_float32_b64(b64: str) -> list[float]:
    """Decode a base64 float32 little-endian encoded vector to ``list[float]``."""
    raw = base64.b64decode(b64)
    if len(raw) % 4 != 0:
        raise ValueError("invalid float32 byte length")
    count = len(raw) // 4
    return list(struct.unpack(f"<{count}f", raw))
