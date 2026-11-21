"""GuardCache: the one connected ActionGuard of each browser profile."""

import asyncio
import contextlib
import logging

from swarm.browser.chrome import ChromeRuntime
from swarm.browser.guard import ActionGuard

log = logging.getLogger(__name__)


class GuardCache:
    """GuardProvider: the ONE ActionGuard of each profile's browser while its CDP connection
    lives (pool contract); the pool arms it per session. A guard whose connection is gone
    (Chrome crashed and was relaunched) holds no live arms, epoch or usable tokens: it is
    closed and replaced by a fresh one, so the profile recovers without a daemon restart."""

    def __init__(self, chrome: ChromeRuntime, allowlist: tuple[str, ...]) -> None:
        self._chrome = chrome
        self._allowlist = list(allowlist)
        self._guards: dict[str, ActionGuard] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    async def __call__(self, browser_id: str, profile: str) -> ActionGuard:
        async with self._locks.setdefault(profile, asyncio.Lock()):
            g = self._guards.get(profile)
            if g is not None and not getattr(g, "connected", True):
                log.warning("guard for profile %r lost its connection: opening a new one", profile)
                self._guards.pop(profile, None)
                with contextlib.suppress(Exception):
                    await g.close()
                g = None
            if g is None:
                g = await self._chrome.open_guard(profile, self._allowlist)
                self._guards[profile] = g
            return g

    async def aclose(self) -> None:
        for g in list(self._guards.values()):
            with contextlib.suppress(Exception):
                await g.close()
        self._guards.clear()
