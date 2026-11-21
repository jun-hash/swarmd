"""Blob store: large payloads and screenshots on disk, pruned by age."""

import os
import re
import shutil
from datetime import datetime
from pathlib import Path

from swarm.clock import Clock, SystemClock
from swarm.ids import new_id
from swarm.types import BlobRef

_EXT_RE = re.compile(r"[A-Za-z0-9]{1,8}")
PERMANENT_DIR = "permanent"


class BlobStore:
    def __init__(self, root: Path, clock: Clock | None = None) -> None:
        self.root = root
        self._clock = clock or SystemClock()
        self.root.mkdir(parents=True, exist_ok=True)

    def put(self, data: bytes, ext: str, *, permanent: bool = False) -> BlobRef:
        """Store bytes. `permanent` blobs (e.g. spilled event payloads) are never pruned."""
        if not _EXT_RE.fullmatch(ext):
            raise ValueError(f"invalid blob ext: {ext!r}")
        ref = f"{new_id('blob')}.{ext}"
        d = self.root / PERMANENT_DIR if permanent else self.root
        d.mkdir(exist_ok=True)
        p = d / ref
        p.write_bytes(data)
        ts = self._clock.now().timestamp()
        os.utime(p, (ts, ts))  # mtime = put time on the injected clock
        return ref

    def path(self, ref: BlobRef) -> Path:
        if Path(ref).name != ref:
            raise ValueError(f"invalid blob ref: {ref!r}")
        perm = self.root / PERMANENT_DIR / ref
        return perm if perm.exists() else self.root / ref

    def prune(self, now: datetime, keep_days: int) -> int:
        cutoff = now.timestamp() - keep_days * 86400
        removed = 0
        for p in self.root.glob("blob_*"):
            if p.is_file() and p.stat().st_mtime < cutoff:
                p.unlink()
                removed += 1
        return removed

    def free_bytes(self) -> int:
        return shutil.disk_usage(self.root).free
