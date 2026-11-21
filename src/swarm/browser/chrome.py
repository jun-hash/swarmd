"""ChromeRuntime: one Chrome process per profile, one dedicated target per session.

Profiles stay warm: closing a profile's last session leaves its browser running; only
`shutdown()` kills browsers. A lost CDP connection is re-established with backoff
5s / 30s / 2m (relaunching Chrome if the process died); after 3 failed attempts the runtime
records `browser.disconnected` and raises `BrowserUnavailable`.
"""

import asyncio
import contextlib
import logging
import os
import signal
import socket
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from pathlib import Path

from swarm.browser.cdp import CdpClient, CdpError
from swarm.browser.guard import ActionGuard
from swarm.clock import Clock, SystemClock
from swarm.ids import new_id
from swarm.store.events import EventLog
from swarm.types import BrowserHealth, BrowserId, CdpSession, Event

log = logging.getLogger(__name__)

CAPABILITIES = frozenset({"screenshot", "a11y_tree", "fetch_intercept", "takeover", "multi_tab"})
RECONNECT_DELAYS_S = (5, 30, 120)


class BrowserUnavailable(RuntimeError):
    """The browser for a profile could not be started or reconnected."""


@dataclass
class _Browser:
    profile: str
    proc: asyncio.subprocess.Process | None
    ws_url: str
    client: CdpClient


@dataclass
class _Session:
    profile: str
    target_id: str
    purpose: str


class ChromeRuntime:
    id: BrowserId = "chrome"

    def __init__(
        self,
        binary: Path,
        profiles: dict[str, Path],
        log: EventLog,
        *,
        headless: bool = False,
        extra_args: Sequence[str] = (),
        clock: Clock | None = None,
        reconnect_delays: Sequence[float] = RECONNECT_DELAYS_S,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        launch_timeout_s: float = 20.0,
    ) -> None:
        self.binary = binary
        self.profiles = dict(profiles)
        self.headless = headless
        self.extra_args = list(extra_args)
        self._log = log
        self._clock = clock or SystemClock()
        self._delays = tuple(reconnect_delays)
        self._sleep = sleep
        self._launch_timeout_s = launch_timeout_s
        self._browsers: dict[str, _Browser] = {}
        self._sessions: dict[str, _Session] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    # --- BrowserRuntime ------------------------------------------------------------------

    async def open_session(self, *, profile: str, purpose: str) -> CdpSession:
        b = await self._ensure_browser(profile)
        r = await b.client.send("Target.createTarget", {"url": "about:blank"})
        sid = new_id("bsess")
        self._sessions[sid] = _Session(profile=profile, target_id=r["targetId"], purpose=purpose)
        return CdpSession(
            id=sid,
            cdp_url=b.ws_url,
            target_id=r["targetId"],
            profile=profile,
            capabilities=CAPABILITIES,
            guard="off",  # nothing intercepts yet: ActionGuard.arm() sets the real mode
        )

    async def close_session(self, session_id: str) -> None:
        """Close the session's target. Never raises; the profile's browser stays running."""
        try:
            s = self._sessions.pop(session_id, None)
            if s is None:
                return
            b = self._browsers.get(s.profile)
            if b is not None and b.client.connected:
                await b.client.send("Target.closeTarget", {"targetId": s.target_id}, timeout_s=5)
        except Exception:
            log.warning("close_session %s failed", session_id, exc_info=True)

    def session_alive(self, session_id: str) -> bool:
        """False once the session's target is gone with its browser (crash, kill, a failed
        reconnect) or was closed: the caller must open a new session, never reuse it."""
        s = self._sessions.get(session_id)
        b = self._browsers.get(s.profile) if s is not None else None
        return b is not None and b.client.connected and _alive(b.proc)

    async def health(self) -> BrowserHealth:
        profiles: dict[str, bool] = {}
        version: str | None = None
        for name, b in list(self._browsers.items()):
            ok = False
            if _alive(b.proc) and b.client.connected:
                try:
                    v = await b.client.send("Browser.getVersion", timeout_s=3)
                    version = version or v.get("product")
                    ok = True
                except Exception:
                    ok = False
            profiles[name] = ok
        return BrowserHealth(
            browser=self.id,
            ok=self.binary.exists() and all(profiles.values()),
            profiles=profiles,
            sessions=len(self._sessions),
            version=version,
        )

    # --- extras ----------------------------------------------------------------------------

    async def connect_client(self, profile: str) -> CdpClient:
        """A new, independent CDP connection to the profile's browser (e.g. for ActionGuard).

        The caller owns it and must `close()` it.
        """
        b = await self._ensure_browser(profile)
        return await CdpClient(b.ws_url).connect()

    async def open_guard(self, profile: str, allowlist: list[str]) -> ActionGuard:
        """An ActionGuard on its own connection, wired to fail closed through this runtime."""
        client = await self.connect_client(profile)

        async def lost(targets: list[str], workers: list[str]) -> bool:
            return await self.fail_closed(profile, targets, workers)

        return ActionGuard(client, self._log, allowlist, clock=self._clock, on_lost=lost)

    async def fail_closed(
        self, profile: str, target_ids: list[str], worker_ids: Sequence[str] = ()
    ) -> bool:
        """Close pages (and service/shared workers) that lost their guard.

        If a page cannot be closed, kill the profile's browser. Workers are closed best-effort:
        Chrome refuses `Target.closeTarget` for some worker types; those are recorded.
        Returns False when nothing was done (unknown profile): the caller must act itself."""
        b = self._browsers.get(profile)
        if b is None:
            log.error(
                "fail_closed: unknown profile %r; targets %s left to the caller",
                profile,
                target_ids,
            )
            return False
        workers_unclosed: list[str] = []
        for wid in worker_ids if b.client.connected else []:
            try:
                await b.client.send("Target.closeTarget", {"targetId": wid}, timeout_s=5)
            except Exception:
                workers_unclosed.append(wid)
        ok = b.client.connected
        for tid in target_ids if ok else []:
            try:
                await b.client.send("Target.closeTarget", {"targetId": tid}, timeout_s=5)
            except CdpError:
                try:  # already gone is fine; still there is not
                    infos = (await b.client.send("Target.getTargets", timeout_s=5))["targetInfos"]
                    ok = ok and all(t["targetId"] != tid for t in infos)
                except Exception:
                    ok = False
            except Exception:
                ok = False
        for sid in [k for k, s in self._sessions.items() if s.target_id in target_ids]:
            del self._sessions[sid]
        if not ok:
            self._browsers.pop(profile, None)
            self._drop_sessions(profile)
            await self._kill(b)
        self._record(
            "browser.fail_closed",
            {
                "profile": profile,
                "targets": target_ids,
                "workers": list(worker_ids),
                "workers_unclosed": workers_unclosed,
                "killed": not ok,
            },
        )
        return True

    def pids(self) -> dict[str, int]:
        return {n: b.proc.pid for n, b in self._browsers.items() if b.proc is not None}

    async def shutdown(self) -> None:
        """Kill every browser this runtime started. Never raises."""
        browsers = list(self._browsers.values())
        self._browsers.clear()
        self._sessions.clear()
        for b in browsers:
            await self._kill(b)

    # --- internals -------------------------------------------------------------------------

    async def _ensure_browser(self, profile: str) -> _Browser:
        if profile not in self.profiles:
            raise ValueError(f"unknown browser profile: {profile!r}")
        lock = self._locks.setdefault(profile, asyncio.Lock())
        async with lock:
            b = self._browsers.get(profile)
            if b is None:
                b = await self._start(profile)
                self._browsers[profile] = b
            elif not b.client.connected or not _alive(b.proc):
                b = await self._reconnect(profile, b)
            return b

    async def _reconnect(self, profile: str, b: _Browser) -> _Browser:
        last: Exception | None = None
        for attempt, delay in enumerate(self._delays, start=1):
            await self._sleep(delay)
            try:
                if _alive(b.proc):
                    with contextlib.suppress(Exception):
                        await b.client.close()
                    b.client = await CdpClient(b.ws_url).connect()
                    fresh = b
                else:
                    self._drop_sessions(profile)  # the old process took its targets with it
                    fresh = await self._start(profile)
                self._browsers[profile] = fresh
                self._record("browser.reconnected", {"profile": profile, "attempt": attempt})
                return fresh
            except Exception as e:
                last = e
                log.warning("reconnect %s attempt %d failed: %s", profile, attempt, e)
        self._browsers.pop(profile, None)
        self._drop_sessions(profile)
        await self._kill(b)
        self._record(
            "browser.disconnected",
            {"profile": profile, "attempts": len(self._delays), "error": repr(last)},
        )
        raise BrowserUnavailable(f"chrome profile {profile!r} unreachable: {last!r}")

    async def _start(self, profile: str) -> _Browser:
        user_dir = self.profiles[profile]
        user_dir.mkdir(parents=True, exist_ok=True)
        port_file = user_dir / "DevToolsActivePort"
        port_file.unlink(missing_ok=True)  # stale file from an earlier run
        await self._kill_orphan(profile)
        args = [
            str(self.binary),
            "--remote-debugging-port=0",
            f"--user-data-dir={user_dir}",
            "--no-first-run",
            "--no-default-browser-check",
        ]
        if self.headless:
            args.append("--headless=new")
        args += [*self.extra_args, "about:blank"]
        try:
            proc = await asyncio.create_subprocess_exec(
                *args,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
                start_new_session=True,  # own process group: shutdown kills helpers too
            )
        except OSError as e:
            raise BrowserUnavailable(f"cannot start chrome: {e}") from e
        b = _Browser(profile=profile, proc=proc, ws_url="", client=CdpClient(""))
        try:
            b.ws_url = await self._read_port_file(port_file, proc)
            b.client = await CdpClient(b.ws_url).connect()
        except BaseException:
            await self._kill(b)
            raise
        self._record("browser.started", {"profile": profile, "pid": proc.pid, "ws": b.ws_url})
        return b

    async def _kill_orphan(self, profile: str) -> None:
        """Kill a Chrome left running on this profile by a previous swarmd.

        Chrome's `SingletonLock` symlink points at "<hostname>-<pid>". The pid is only killed
        when it is on this host, alive, and its command line names this profile directory."""
        user_dir = self.profiles[profile]
        try:
            target = os.readlink(user_dir / "SingletonLock")
        except OSError:
            return
        host, _, pid_s = target.rpartition("-")
        if not pid_s.isdigit() or host != socket.gethostname():
            return
        pid = int(pid_s)
        if pid == os.getpid() or not _pid_alive(pid):
            return
        try:
            ps = await asyncio.create_subprocess_exec(
                "ps",
                "-p",
                str(pid),
                "-o",
                "command=",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
            out, _ = await ps.communicate()
        except OSError:
            return
        if f"--user-data-dir={user_dir}" not in out.decode(errors="replace"):
            return
        for sig in (signal.SIGTERM, signal.SIGKILL):
            try:
                os.killpg(pid, sig)  # previous swarmd started it as a group leader
            except (ProcessLookupError, PermissionError):
                with contextlib.suppress(ProcessLookupError, PermissionError):
                    os.kill(pid, sig)
            for _ in range(100):
                if not _pid_alive(pid):
                    break
                await asyncio.sleep(0.05)
            if not _pid_alive(pid):
                break
        self._record("browser.orphan_killed", {"profile": profile, "pid": pid})

    async def _read_port_file(self, port_file: Path, proc: asyncio.subprocess.Process) -> str:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._launch_timeout_s
        while loop.time() < deadline:
            if proc.returncode is not None:
                raise BrowserUnavailable(f"chrome exited early (code {proc.returncode})")
            try:
                lines = port_file.read_text().splitlines()
            except OSError:
                lines = []
            if len(lines) >= 2 and lines[0].strip().isdigit() and lines[1].startswith("/"):
                return f"ws://127.0.0.1:{lines[0].strip()}{lines[1].strip()}"
            await asyncio.sleep(0.05)
        raise BrowserUnavailable(f"no DevToolsActivePort after {self._launch_timeout_s}s")

    async def _kill(self, b: _Browser) -> None:
        with contextlib.suppress(Exception):
            await b.client.close()
        proc = b.proc
        if proc is None or proc.returncode is not None:
            return
        for sig, wait_s in ((signal.SIGTERM, 5.0), (signal.SIGKILL, 5.0)):
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(proc.pid, sig)
            try:
                await asyncio.wait_for(proc.wait(), wait_s)
                break
            except TimeoutError:
                continue
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(proc.pid, signal.SIGKILL)  # helpers that outlived the leader

    def _drop_sessions(self, profile: str) -> None:
        for sid in [k for k, s in self._sessions.items() if s.profile == profile]:
            del self._sessions[sid]

    def _record(self, kind: str, payload: dict) -> None:
        try:
            self._log.append(
                Event(
                    id="",
                    ts=self._clock.now(),
                    kind=kind,
                    actor="browser.chrome",
                    project_id=None,
                    run_id=None,
                    payload=payload,
                    refs=[],
                )
            )
        except Exception:
            log.exception("failed to record %s", kind)


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _alive(proc: asyncio.subprocess.Process | None) -> bool:
    return proc is not None and proc.returncode is None
