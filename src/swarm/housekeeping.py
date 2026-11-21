"""The daemon's own background loops: the hourly login check and the daily blob prune.

Quota is not pre-managed: the auth monitor only feeds the ledger the auth
state (auth_failed stops every lease until auth_ok) and tells General once per episode.
"""

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable
from datetime import datetime, time
from pathlib import Path

from swarm.aio import pause
from swarm.clock import Clock
from swarm.engine.claude import auth_status
from swarm.scheduler.ledger import SqliteQuotaLedger
from swarm.store.blobs import BlobStore
from swarm.store.kv import KvTable
from swarm.types import ChannelRef, EngineId, UsageSignal

log = logging.getLogger(__name__)

AUTH_CHECK_EVERY_S = 3600.0  # `claude auth status`: the hourly re-login check
AUTH_POLL_S = 60.0  # notice an auth failure the engine itself detected mid-run
AUTH_LOST = (
    "⚠️ FYI · claude 로그인이 풀렸어요 — 새 작업을 멈췄어요.\n"
    "맥미니 터미널에서 `claude` 로 다시 로그인해 주세요. 1시간마다 확인해서 자동으로 다시 시작해요."
)
AUTH_BACK = "ℹ️ FYI · claude 로그인이 돌아왔어요 — 작업을 다시 시작해요."

PRUNE_AT = time(3, 0)
PRUNE_POLL_S = 60.0
KEEP_DAYS = 30  # screenshots are kept 30 days
LOW_DISK_KEEP_DAYS = 7
LOW_DISK_BYTES = 5 * 1024**3

# (channel, text, urgent): queue a system FYI
Fyi = Callable[[ChannelRef, str, bool], Awaitable[None]]


# --- auth monitor --------------------------------------------------------------------------------


async def default_auth_check(binary: str) -> bool:
    """`claude auth status` through the engine's own runner: ANTHROPIC_API_KEY /
    ANTHROPIC_AUTH_TOKEN stripped, never `--bare` (subscription login lives in the keychain)."""
    return await auth_status(binary)


class AuthMonitor:
    """Hourly login check. Logged out -> ledger `auth_failed` (no lease for anyone until
    `auth_ok`; no retry loop) + ONE urgent FYI to General per episode (also when the engine
    itself detected the failure mid-run). Back -> `auth_ok` + a short FYI."""

    def __init__(
        self, *, ledger: SqliteQuotaLedger, kv: KvTable, check: Callable[[], Awaitable[bool]],
        fyi: Fyi, general: ChannelRef, clock: Clock, engine_id: EngineId,
        every_s: float = AUTH_CHECK_EVERY_S, poll_s: float = AUTH_POLL_S,
    ) -> None:  # fmt: skip
        self._ledger = ledger
        self._engine_id = engine_id
        self._kv = kv
        self._check = check
        self._fyi = fyi
        self._general = general
        self._clock = clock
        self._every_s = every_s
        self._poll_s = poll_s

    def _signal(self, kind: str) -> UsageSignal:
        return UsageSignal(kind=kind, reset_at=None, at=self._clock.now())  # type: ignore[typeddict-item]

    async def check_once(self) -> bool:
        try:
            ok = await self._check()
        except Exception:  # noqa: BLE001 - a broken check is not proof of a logout
            log.warning("auth check failed to run", exc_info=True)
            return not self._ledger.disabled(self._engine_id)
        if not ok:
            self._ledger.observe(engine=self._engine_id, signal=self._signal("auth_failed"))
        elif self._ledger.disabled(self._engine_id):
            self._ledger.observe(engine=self._engine_id, signal=self._signal("auth_ok"))
        await self.sync_alert()
        return ok

    async def sync_alert(self) -> None:
        disabled = self._ledger.disabled(self._engine_id)
        alerted = self._kv.get("auth_alerted") is not None
        if disabled and not alerted:
            self._kv.set("auth_alerted", self._clock.now().isoformat())  # once, even on restart
            await self._fyi(self._general, AUTH_LOST, True)
        elif not disabled and alerted:
            self._kv.set("auth_alerted", None)
            await self._fyi(self._general, AUTH_BACK, False)

    async def run(self, stop: asyncio.Event) -> None:
        loop = asyncio.get_running_loop()
        next_check = loop.time()
        while not stop.is_set():
            try:
                if loop.time() >= next_check:
                    next_check = loop.time() + self._every_s
                    await self.check_once()
                else:
                    await self.sync_alert()
            except Exception:  # noqa: BLE001 - the loop outlives a busy DB
                log.exception("auth monitor failed")
            if await pause(min(self._poll_s, max(next_check - loop.time(), 0.0)), stop):
                return


# --- blob janitor --------------------------------------------------------------------------------


class BlobJanitor:
    """Daily at 03:00 (configured timezone): drop blobs and screenshots older than 30 days;
    with less than 5 GB free keep only 7 days and tell General. A prune missed while swarmd
    was down runs at the next start after 03:00."""

    def __init__(
        self, *, blobs: BlobStore, shots_dir: Path, kv: KvTable, fyi: Fyi, general: ChannelRef,
        clock: Clock, free_bytes: Callable[[], int] | None = None,
    ) -> None:  # fmt: skip
        self._blobs = blobs
        self._shots = shots_dir
        self._kv = kv
        self._fyi = fyi
        self._general = general
        self._clock = clock
        self._free = free_bytes or blobs.free_bytes

    def due(self, now: datetime) -> bool:
        return now.time() >= PRUNE_AT and self._kv.get("prune_date") != now.date().isoformat()

    def _prune_shots(self, now: datetime, keep_days: int) -> int:
        cutoff = now.timestamp() - keep_days * 86400
        removed = 0
        for p in self._shots.glob("shot_*") if self._shots.is_dir() else ():
            with contextlib.suppress(OSError):
                if p.is_file() and p.stat().st_mtime < cutoff:
                    p.unlink()
                    removed += 1
        return removed

    async def prune_once(self, now: datetime) -> tuple[int, int]:
        """-> (keep_days used, files removed)."""
        free = self._free()
        low = free < LOW_DISK_BYTES
        keep = LOW_DISK_KEEP_DAYS if low else KEEP_DAYS
        removed = self._blobs.prune(now, keep_days=keep) + self._prune_shots(now, keep)
        self._kv.set("prune_date", now.date().isoformat())
        log.info("blob prune: keep %s days, %s removed, %.1f GB free", keep, removed, free / 1e9)
        if low:
            await self._fyi(
                self._general,
                f"⚠️ FYI · 디스크 여유 {free / 1024**3:.1f}GB (< 5GB) — 스크린샷 보존을 "
                f"{LOW_DISK_KEEP_DAYS}일로 줄였어요 ({removed}개 삭제). 맥미니 정리가 필요해요.",
                False,
            )
        return keep, removed

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                now = self._clock.now()
                if self.due(now):
                    await self.prune_once(now)
            except Exception:  # noqa: BLE001
                log.exception("blob prune failed")
            if await pause(PRUNE_POLL_S, stop):
                return
