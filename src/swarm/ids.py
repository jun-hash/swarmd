"""Monotonic ULID-based ids: "<prefix>_<26 Crockford base32 chars>"."""

import os
import threading
import time

_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
_lock = threading.Lock()
_last_ms = -1
_last_rand = 0


def _encode(value: int, length: int) -> str:
    chars = []
    for _ in range(length):
        chars.append(_ALPHABET[value & 31])
        value >>= 5
    return "".join(reversed(chars))


def new_id(prefix: str) -> str:
    """Return a ULID-based id; strictly increasing within a process."""
    global _last_ms, _last_rand
    with _lock:
        ms = time.time_ns() // 1_000_000
        if ms <= _last_ms:
            # same (or regressed) millisecond: keep ordering by bumping the random part
            ms = _last_ms
            _last_rand += 1
            if _last_rand >= 1 << 80:  # overflow: move to the next millisecond
                ms += 1
                _last_rand = int.from_bytes(os.urandom(10), "big") >> 1
        else:
            _last_rand = int.from_bytes(os.urandom(10), "big") >> 1  # headroom for increments
        _last_ms = ms
        return f"{prefix}_{_encode(ms, 10)}{_encode(_last_rand, 16)}"
