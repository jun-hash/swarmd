"""swarmd: the one always-on process (assembly, start, stop — nothing else).

Boot: config (fail fast: General channel, bot token, secret) -> single-instance lock
(`fcntl.flock` on <home>/swarmd.lock; held by someone else -> exit 3, before the DB is even
opened) -> assemble every component on one DB -> `pool.recover_on_boot()` (leased / running /
verifying -> interrupted) BEFORE the agenda loop -> start on this one event loop: internal app
(uvicorn, 127.0.0.1 only: internal routes + SwarmFront MCP at /mcp), notifier drain, announce
sweeps, kanban mirror, the housekeeping loops (hourly `claude auth status`, 03:00 blob prune)
and the agenda loop. `<home>/heartbeat` is touched after every completed agenda tick (the
watchdog restarts swarmd when it goes stale).

Shutdown (SIGTERM / SIGINT), bounded by SHUTDOWN_BUDGET_S (each step also by a per-step cap,
so one hung step cannot eat the others' time; launchd's ExitTimeOut is larger):

  stop -> pool.aclose (live runs end `interrupted`, resumable after the restart) -> loops
  exit (agenda, mirror.run, sweeps, drain, ...) -> mirror.aclose -> coordinator.aclose
  (tracked tasks) -> pool.aclose again (anything started meanwhile; shielded teardowns,
  cancellers, releasers) -> front / api drains -> one last outbox flush -> notifier.aclose
  -> uvicorn exit -> guards + Chrome closed -> DB closed -> lock released.
"""

import argparse
import asyncio
import contextlib
import fcntl
import json
import logging
import os
import signal
import sys
import time as wallclock
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx

from swarm.brain import EngineBrain
from swarm.browser.chrome import ChromeRuntime
from swarm.browser.guards import GuardCache
from swarm.clock import AlertWindow, Clock, SystemClock
from swarm.config import Config, load_config
from swarm.coordinator.coordinator import Coordinator
from swarm.engine.base import Engine
from swarm.engine.registry import build_engine
from swarm.engine.simulate import wrap_limit_simulation
from swarm.front.internal_api import InternalApi, build_app, make_server
from swarm.front.kanban import HermesKanbanMirror, WsConnect
from swarm.front.mcp_server import SwarmFrontService, build_mcp, streamable_app
from swarm.front.notifier import TelegramNotifier, parse_channel
from swarm.harness.own.context import RolloverBrief
from swarm.harness.registry import COMPATIBLE, HarnessDeps, HarnessRegistry
from swarm.housekeeping import AuthMonitor, BlobJanitor, default_auth_check
from swarm.ids import new_id
from swarm.inbox.inbox import SqliteInbox
from swarm.ports import BrowserRuntime
from swarm.projects.store import SqliteProjectStore
from swarm.scheduler.ledger import SqliteQuotaLedger
from swarm.store.blobs import BlobStore
from swarm.store.db import Database
from swarm.store.events import Emitter, SqliteEventLog
from swarm.store.kv import KvTable
from swarm.types import (
    BrowserId,
    ChannelRef,
    Event,
    Outbound,
    ProjectId,
    Variant,
)
from swarm.workers.pool import GuardProvider, HarnessSource, WorkerPool
from swarm.workers.verify import Verifier

log = logging.getLogger("swarmd")

EXIT_OK, EXIT_ERROR, EXIT_CONFIG, EXIT_LOCKED = 0, 1, 2, 3
DEFAULT_CONFIG = "~/swarm/config.toml"
LOCK_NAME = "swarmd.lock"
HEARTBEAT_NAME = "heartbeat"
WATCHDOG_STATE = "watchdog.json"
LAUNCHD_LABEL = "com.swarm.swarmd"
SECRET_NAME = "secret"

SHUTDOWN_BUDGET_S = 30.0  # < ExitTimeOut (40 s) in com.swarm.swarmd.plist
SHUTDOWN_STEP_CAP_S = 10.0
BACKSTOP_GRACE_S = 5.0  # budget + this: a cancellation-resistant step gets os._exit
LAUNCHD_RETRY_PAUSE_S = 60.0  # exit 2/3 under launchd: no respawn every ThrottleInterval
FLUSH_CAP_S = 5.0  # last outbox drain attempt
HTTP_START_S = 15.0
MIRROR_LOOP_RETRY_S = (5.0, 300.0)
TASK_ERROR_MAX = 500  # a dead task's error in `daemon.task_died`

_SCHEMA = "CREATE TABLE daemon_kv (key TEXT PRIMARY KEY, value TEXT NOT NULL)"


class ConfigError(Exception):
    """Startup cannot go on: a clear, human-readable reason (exit 2)."""


# --- config / secret / lock ------------------------------------------------------------------


def default_config_path() -> Path:
    return Path(os.environ.get("SWARM_CONFIG", DEFAULT_CONFIG)).expanduser()


def load_and_validate(path: Path) -> Config:
    try:
        cfg = load_config(path)
    except FileNotFoundError:
        raise ConfigError(f"config not found: {path} (run `swarm init`)") from None
    except KeyError as e:
        raise ConfigError(f"config {path}: missing key {e.args[0]!r}") from None
    except Exception as e:  # noqa: BLE001 - TOML syntax and value errors
        raise ConfigError(f"config {path}: {type(e).__name__}: {e}") from None
    if not str(cfg.general_channel or "").strip():
        raise ConfigError(
            "general_channel is not set in config.toml — set it to the Telegram General "
            "topic ('telegram:<chat_id>:1'). Without it, Asks whose project topic fails and "
            "every system FYI would have nowhere to go."
        )
    try:
        parse_channel(cfg.general_channel)
    except ValueError as e:
        raise ConfigError(f"general_channel: {e}") from None
    try:
        ZoneInfo(cfg.timezone)
    except (ZoneInfoNotFoundError, ValueError, TypeError):
        raise ConfigError(f"timezone: unknown zone {cfg.timezone!r}") from None
    if not cfg.telegram_bot_token.strip():
        raise ConfigError("telegram_bot_token is empty in config.toml (from @BotFather)")
    if not cfg.profiles:
        raise ConfigError(
            'no browser [profiles] in config.toml (e.g. work = "~/swarm/chrome/work")'
        )
    return cfg


def read_secret(home: Path) -> str:
    p = home / SECRET_NAME
    try:
        secret = p.read_text().strip()
    except FileNotFoundError:
        raise ConfigError(f"internal API secret missing: {p} (run `swarm init`)") from None
    if not secret:
        raise ConfigError(f"internal API secret is empty: {p}")
    if p.stat().st_mode & 0o077:
        log.warning("%s was readable by others; set to 0600", p)
        p.chmod(0o600)
    return secret


class InstanceLock:
    """`fcntl.flock` on <home>/swarmd.lock: launchd and a manual start must never both run
    leases and Runs. The kernel drops it when the process dies."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._fd: int | None = None

    def acquire(self) -> bool:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:  # BlockingIOError: someone else holds it
            os.close(fd)
            return False
        os.ftruncate(fd, 0)
        os.write(fd, f"{os.getpid()}\n".encode())
        self._fd = fd
        return True

    def release(self) -> None:
        if self._fd is None:
            return
        with contextlib.suppress(OSError):
            fcntl.flock(self._fd, fcntl.LOCK_UN)
        with contextlib.suppress(OSError):
            os.close(self._fd)
        self._fd = None


# --- seams (tests swap adapters; production leaves them None) -----------------------------------


@dataclass
class Seams:
    """What a test (or an embedder) may swap; the defaults are production.

    Three groups: adapter swaps (None builds the real adapter), timing knobs (the agenda
    tick and the shutdown bounds), and test hooks (stop without a signal, observe the
    started daemon, replace the hard exit, skip the HTTP server)."""

    # --- adapter swaps
    clock: Clock | None = None
    engine: Engine | None = None  # replaces ClaudeEngine (worker + Brain + Verifier)
    browser: BrowserRuntime | None = None  # replaces ChromeRuntime
    guards: GuardProvider | None = None
    harnesses: HarnessSource | None = None
    telegram_transport: httpx.AsyncBaseTransport | None = None
    hermes_transport: httpx.AsyncBaseTransport | None = None
    ws_connect: WsConnect | None = None
    auth_check: Callable[[], Awaitable[bool]] | None = None
    free_bytes: Callable[[], int] | None = None
    # --- timing knobs
    tick_every_s: float | None = None
    shutdown_budget_s: float = SHUTDOWN_BUDGET_S
    shutdown_step_cap_s: float = SHUTDOWN_STEP_CAP_S
    backstop_grace_s: float = BACKSTOP_GRACE_S
    # --- test hooks
    stop: asyncio.Event | None = None  # tests stop the daemon without a signal
    on_started: Callable[["Daemon"], None] | None = None
    force_exit: Callable[[], None] | None = None  # default: os._exit(EXIT_ERROR)
    serve_http: bool = True


def daemon_kv(db: Database) -> KvTable:
    db.migrate("daemon_v1", _SCHEMA)
    return KvTable(db, "daemon_kv")


# --- the daemon ----------------------------------------------------------------------------------


def default_variant(cfg: Config, browser: BrowserId) -> Variant:
    """The variant a project or order without its own runs with."""
    return Variant(
        harness="own", engine="claude", browser=browser,
        context_policy=RolloverBrief.id, model=cfg.claude_model,
    )  # fmt: skip


def _browser_mcp_entry() -> str:
    """The worker's stdio MCP server: the `swarm-browser-mcp` script next to this Python."""
    return str(Path(sys.executable).parent / "swarm-browser-mcp")


_ShutdownStep = tuple[str, Callable[[], Awaitable[Any]], float]  # name, step, its bound (s)


@dataclass
class _Running:
    tasks: dict[str, asyncio.Task] = field(default_factory=dict)
    http: asyncio.Task | None = None
    server: Any = None


class Daemon:
    """Every component, assembled once on one Database. `serve(stop)` boots and runs until
    stop, then shuts down in order; `trace` records boot and shutdown steps (tests, logs)."""

    def __init__(self, cfg: Config, secret: str, seams: Seams | None = None) -> None:
        self.cfg = cfg
        self.seams = seams or Seams()
        self.trace: list[str] = []
        # Order matters: each builder reads what the one before it set on `self`, and the
        # construction order is the database migration order. Do not reorder them.
        self._build_core()
        self._build_workers()
        self._build_notifier()
        self._build_coordinator()  # needs the notifier, so it sits between it and the fronts
        self._build_fronts(secret)
        self._build_housekeeping()
        self.exit_code = EXIT_OK

    # --- assembly --------------------------------------------------------------------------

    def _build_core(self) -> None:
        """Storage, the event log, projects, the ledger, the browser, the engine and the brain."""
        cfg, s, home = self.cfg, self.seams, self.cfg.home
        for d in ("logs", "blobs", "runs", "engine", "projects"):
            (home / d).mkdir(parents=True, exist_ok=True)
        self.heartbeat_path = home / HEARTBEAT_NAME
        self.tz = tz = ZoneInfo(cfg.timezone)
        self.clock = clock = s.clock or SystemClock(tz)
        self.db = db = Database(home / "swarm.db")
        self.blobs = BlobStore(home / "blobs", clock)
        self.shots_dir = home / "blobs" / "shots"
        self.shots_dir.mkdir(parents=True, exist_ok=True)
        self.log = ev_log = SqliteEventLog(db, self.blobs, tz)
        self.kv = daemon_kv(db)
        self._emit = Emitter(ev_log, clock, "daemon", best_effort=True)
        self.projects = SqliteProjectStore(db, ev_log, home, clock)
        self.ledger = ledger = SqliteQuotaLedger(db, ev_log, clock, instance_id=new_id("inst"))
        self.browser = browser = s.browser or ChromeRuntime(
            cfg.chrome_binary, cfg.profiles, ev_log, clock=clock
        )
        self.default_variant = variant = default_variant(cfg, browser.id)
        assert variant["engine"] is not None  # default_variant always names one
        self.engine = engine = wrap_limit_simulation(  # soak hook: inert unless the env is set
            s.engine or build_engine(variant["engine"], cfg, home / "engine", clock), clock
        )
        self.brain = EngineBrain(engine, ledger, ev_log, clock=clock)

    def _build_workers(self) -> None:
        """The inbox, the guards, the harnesses and the worker pool."""
        cfg, s, home, clock = self.cfg, self.seams, self.cfg.home, self.clock
        db, ev_log, projects, ledger = self.db, self.log, self.projects, self.ledger
        engine, browser = self.engine, self.browser
        self.inbox = inbox = SqliteInbox(
            db, ev_log, projects, self.brain, clock,
            # the coordinator and the pool are built after the inbox: look them up per call
            run_project=lambda r: self.coordinator.ask_project(r) or self.pool.run_project(r),
            general_channel=cfg.general_channel,
        )  # fmt: skip
        self.guards = s.guards or GuardCache(browser, cfg.guard_allowlist)  # type: ignore[arg-type]
        self.internal_url = f"http://127.0.0.1:{cfg.internal_port}"
        harnesses = s.harnesses or HarnessRegistry(
            HarnessDeps(
                mcp_entry=_browser_mcp_entry(),
                internal_url=self.internal_url,
                token_file=home / SECRET_NAME,  # the worker's bearer = the internal secret
                context_policy=RolloverBrief(),
                blob_dir=self.shots_dir,
                clock=clock,
            )
        )
        self.pool = WorkerPool(
            db=db, projects=projects, inbox=inbox, ledger=ledger, engines={engine.id: engine},
            compatible=COMPATIBLE, browsers={browser.id: browser}, guards=self.guards,
            harnesses=harnesses, log=ev_log, clock=clock, output_root=home / "runs",
            verifier=Verifier(engine, ledger, blob_root=self.shots_dir),
        )  # fmt: skip

    def _build_notifier(self) -> None:
        """The Telegram notifier, with the alert window in the configured time zone."""
        cfg, s = self.cfg, self.seams
        window = AlertWindow(cfg.alert_start, cfg.alert_end, self.tz)
        self.notifier = TelegramNotifier(
            cfg.telegram_bot_token, self.db, self.inbox, self.clock, window,
            log=self.log, general_channel=cfg.general_channel,  # Ask fallback: never None here
            transport=s.telegram_transport,
        )  # fmt: skip

    def _build_coordinator(self) -> None:
        """The orchestration layer: the agenda loop, tells, reports."""
        cfg, s = self.cfg, self.seams
        profile = "work" if "work" in cfg.profiles else next(iter(cfg.profiles))
        extra = {"tick_every_s": s.tick_every_s} if s.tick_every_s is not None else {}
        self.coordinator = Coordinator(
            db=self.db, log=self.log, projects=self.projects, inbox=self.inbox,
            ledger=self.ledger, pool=self.pool, brain=self.brain, notifier=self.notifier,
            clock=self.clock, general_channel=cfg.general_channel, default_profile=profile,
            default_variant=self.default_variant, on_tick=self.touch_heartbeat, **extra,
        )  # fmt: skip

    def _build_fronts(self, secret: str) -> None:
        """The internal API, SwarmFront, the HTTP app and the kanban mirror."""
        cfg, s, clock, db, ev_log = self.cfg, self.seams, self.clock, self.db, self.log
        projects, inbox, pool, notifier = self.projects, self.inbox, self.pool, self.notifier
        self.api = InternalApi(
            db=db, pool=pool, inbox=inbox, projects=projects, notifier=notifier, clock=clock,
            general_channel=cfg.general_channel, log=ev_log,
        )  # fmt: skip
        self.front = SwarmFrontService(
            coordinator=self.coordinator, inbox=inbox, projects=projects, notifier=notifier
        )
        self.app = build_app(
            secret=secret, api=self.api, mcp_app=streamable_app(build_mcp(self.front))
        )
        self.mirror = HermesKanbanMirror(
            cfg.hermes_dashboard_url, db, ev_log, clock, projects=projects, runs=pool,
            token=cfg.hermes_session_token, transport=s.hermes_transport,
            ws_connect=s.ws_connect, loop_retry_s=MIRROR_LOOP_RETRY_S, alert=self.mirror_alert,
        )  # fmt: skip

    def _build_housekeeping(self) -> None:
        """The daemon's own loops: the hourly login check and the daily blob prune."""
        cfg, s, clock, kv, fyi = self.cfg, self.seams, self.clock, self.kv, self.fyi
        check = s.auth_check or (lambda: default_auth_check(cfg.claude_binary))
        self.auth = AuthMonitor(
            ledger=self.ledger, kv=kv, check=check, fyi=fyi,
            general=cfg.general_channel, clock=clock, engine_id=self.engine.id,
        )  # fmt: skip
        self.janitor = BlobJanitor(
            blobs=self.blobs, shots_dir=self.shots_dir, kv=kv, fyi=fyi,
            general=cfg.general_channel, clock=clock, free_bytes=s.free_bytes,
        )  # fmt: skip

    # --- small services ------------------------------------------------------------------

    def touch_heartbeat(self) -> None:
        self.heartbeat_path.touch()

    async def fyi(self, channel: ChannelRef, text: str, urgent: bool = False) -> None:
        msg = Outbound(channel=channel, kind="fyi", text=text, ref_id=None, urgent=urgent)  # type: ignore[typeddict-item]
        try:
            await self.notifier.send(msg)
        except ValueError:  # the project's channel is not deliverable: use General
            if channel != self.cfg.general_channel:
                with contextlib.suppress(Exception):
                    await self.notifier.send({**msg, "channel": self.cfg.general_channel})
        except Exception:  # noqa: BLE001 - an FYI never breaks a loop
            log.warning("fyi not queued", exc_info=True)

    async def mirror_alert(self, project_id: ProjectId | None, text: str) -> None:
        channel = self.cfg.general_channel
        if project_id:
            with contextlib.suppress(KeyError, ValueError):
                channel = self.projects.get(project_id)["channel"]
        await self.fyi(channel, text, False)

    def log_watchdog_restarts(self) -> int:
        """A heartbeat restart is logged as `watchdog.restart`. The watchdog stays
        read-only on swarm.db, so the restarted daemon copies each restart it has not logged
        yet from <home>/watchdog.json (marker in daemon_kv: never twice)."""
        try:
            raw = json.loads((self.cfg.home / WATCHDOG_STATE).read_text())
            seen = raw.get("history") or [raw["last_restart_at"]]
            times = sorted({datetime.fromisoformat(t) for t in seen if t})
        except (OSError, KeyError, TypeError, ValueError, AttributeError):
            return 0  # a missing or malformed watchdog.json never blocks boot
        mark = self.kv.get("watchdog_logged_until")
        try:
            new = [t for t in times if mark is None or t > datetime.fromisoformat(mark)]
        except (TypeError, ValueError):  # corrupt marker or naive/aware mix: skip, don't crash
            return 0
        for t in new:
            with contextlib.suppress(Exception):
                self.log.append(
                    Event(id="", ts=t, kind="watchdog.restart", actor="watchdog",
                          project_id=None, run_id=None, refs=[],
                          payload={"at": t.isoformat(), "label": LAUNCHD_LABEL,
                                   "restarts_today": raw.get("restarts")})
                )  # fmt: skip
        if new:
            self.kv.set("watchdog_logged_until", new[-1].isoformat())
        return len(new)

    # --- run -------------------------------------------------------------------------------

    async def _serve_http(self, server) -> None:
        try:
            await server.serve()
        except SystemExit as e:  # uvicorn exits the process on a bind error: not ours to do
            raise RuntimeError(
                f"internal API could not start (port {self.cfg.internal_port})"
            ) from e

    async def _start_http(self, run: _Running) -> None:
        if not self.seams.serve_http:
            return
        run.server = server = make_server(self.app, self.cfg.internal_port)
        run.http = asyncio.create_task(self._serve_http(server), name="swarmd-http")
        deadline = asyncio.get_running_loop().time() + HTTP_START_S
        while not server.started:
            if run.http.done():
                run.http.result()  # raises the startup failure
                raise RuntimeError("internal API stopped during startup")
            if asyncio.get_running_loop().time() > deadline:
                raise RuntimeError("internal API did not start in time")
            await asyncio.sleep(0.02)

    async def serve(self, stop: asyncio.Event) -> int:
        run = _Running()
        try:
            recovered = self.pool.recover_on_boot()  # BEFORE the agenda loop (and any lease)
            self.trace.append("recover_on_boot")
            self._emit("daemon.boot", pid=os.getpid(), interrupted=recovered)
            self.log_watchdog_restarts()
            log.info("swarmd boot: %d run(s) left interrupted by the last process", len(recovered))
            await self._start_http(run)
            self.trace.append("http")
            loops: dict[str, Callable[[], Coroutine[Any, Any, Any]]] = {
                "notifier": lambda: self.notifier.drain(stop),
                "sweeps": lambda: self.api.run_sweeps(stop),
                "mirror": lambda: self.mirror.run(stop, submit=self.coordinator.submit),
                "auth": lambda: self.auth.run(stop),
                "janitor": lambda: self.janitor.run(stop),
                "agenda": lambda: self.coordinator.agenda_loop(stop),  # last: everything is up
            }
            for name, start in loops.items():
                run.tasks[name] = asyncio.create_task(start(), name=f"swarmd-{name}")
                self.trace.append(f"start:{name}")
            if self.seams.on_started is not None:
                self.seams.on_started(self)
            await self._until_stop(stop, run)
        except Exception:
            log.exception("swarmd failed")
            self.exit_code = EXIT_ERROR
        finally:
            stop.set()
            await self._shutdown(run)
        return self.exit_code

    async def _until_stop(self, stop: asyncio.Event, run: _Running) -> None:
        stopper = asyncio.create_task(stop.wait())
        watched = [stopper, *run.tasks.values(), *([run.http] if run.http else [])]
        try:
            done, _ = await asyncio.wait(watched, return_when=asyncio.FIRST_COMPLETED)
        finally:
            stopper.cancel()
        if stop.is_set():  # loops end on stop by design (maybe before `stopper` wakes)
            return
        dead = [t for t in done if t is not stopper]
        for t in dead:  # a loop that ends on its own is a bug: exit so launchd restarts us
            err = None if t.cancelled() else t.exception()
            log.error("swarmd task %s ended unexpectedly: %r", t.get_name(), err)
            self._emit("daemon.task_died", task=t.get_name(), error=repr(err)[:TASK_ERROR_MAX])
            self.exit_code = EXIT_ERROR

    def _force_exit(self) -> None:
        log.critical("shutdown overran its budget: forcing exit (the kernel drops the lock)")
        if self.seams.force_exit is not None:
            self.seams.force_exit()
            return
        sys.stderr.flush()
        os._exit(EXIT_ERROR)

    def _shutdown_steps(self, run: _Running) -> list[_ShutdownStep]:
        """(name, step, its own bound in seconds), in the order shutdown runs them."""
        cap = self.seams.shutdown_step_cap_s

        async def loops_exit() -> None:
            tasks = list(run.tasks.values())
            try:
                await asyncio.gather(*tasks, return_exceptions=True)
            finally:  # timed out: cancel what did not stop by itself
                for t in tasks:
                    t.cancel()

        async def http_exit() -> None:
            if run.server is not None:
                run.server.should_exit = True
            if run.http is not None:
                await asyncio.gather(run.http, return_exceptions=True)

        async def browser_exit() -> None:
            close = getattr(self.guards, "aclose", None)
            if close is not None:
                await close()
            shutdown = getattr(self.browser, "shutdown", None)
            if shutdown is not None:
                await shutdown()

        return [
            ("pool.interrupt", self.pool.aclose, cap),  # runs left interrupted first
            ("loops", loops_exit, cap),
            ("mirror.aclose", self.mirror.aclose, cap),  # only after mirror.run returned
            ("coordinator.aclose", self.coordinator.aclose, cap),
            ("pool.aclose", self.pool.aclose, cap),  # teardowns / cancellers started meanwhile
            ("front.aclose", self.front.aclose, cap),
            ("api.drain", self.api.drain, cap),
            ("notifier.flush", self.notifier.drain_once, FLUSH_CAP_S),
            ("notifier.aclose", self.notifier.aclose, cap),
            ("http", http_exit, cap),
            ("browser", browser_exit, cap),
        ]

    async def _shutdown(self, run: _Running) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.seams.shutdown_budget_s
        self._emit("daemon.shutdown")
        # the per-step caps rely on cancellation; a step that swallows it would hang a manual
        # run forever (launchd SIGKILLs at ExitTimeOut): past budget + grace, exit hard
        backstop = loop.call_later(
            self.seams.shutdown_budget_s + self.seams.backstop_grace_s, self._force_exit
        )

        async def step(name: str, make: Callable[[], Awaitable[Any]], limit: float) -> None:
            self.trace.append(name)
            left = min(deadline - loop.time(), limit)
            if left <= 0:
                log.error("shutdown: budget spent, skipping %s", name)
                return
            try:
                await asyncio.wait_for(make(), left)
            except TimeoutError:
                log.error("shutdown: %s timed out after %.1fs", name, left)
            except Exception:  # noqa: BLE001 - every later step still runs
                log.exception("shutdown: %s failed", name)

        for name, make, limit in self._shutdown_steps(run):
            await step(name, make, limit)
        for t in [*run.tasks.values(), *([run.http] if run.http else [])]:
            if not t.done():
                t.cancel()
        backstop.cancel()
        self.trace.append("db.close")
        with contextlib.suppress(Exception):
            self.db.close()
        log.info("swarmd stopped (exit %s)", self.exit_code)


# --- entry points --------------------------------------------------------------------------------


async def main(config_path: Path, *, seams: Seams | None = None) -> int:
    seams = seams or Seams()
    try:
        cfg = load_and_validate(Path(config_path).expanduser())
        secret = read_secret(cfg.home)
    except ConfigError as e:
        print(f"swarmd: {e}", file=sys.stderr)
        return EXIT_CONFIG
    lock = InstanceLock(cfg.home / LOCK_NAME)
    if not lock.acquire():
        print(f"swarmd: another swarmd is running (lock {lock.path}); exiting", file=sys.stderr)
        return EXIT_LOCKED
    loop = asyncio.get_running_loop()
    stop = seams.stop or asyncio.Event()
    installed: list[int] = []
    try:
        for sig in (signal.SIGTERM, signal.SIGINT):
            with contextlib.suppress(NotImplementedError, RuntimeError, ValueError):
                loop.add_signal_handler(sig, stop.set)
                installed.append(sig)
        try:
            daemon = Daemon(cfg, secret, seams)
        except Exception:
            log.exception("swarmd: assembly failed")
            return EXIT_ERROR
        return await daemon.serve(stop)
    finally:
        for sig in installed:
            with contextlib.suppress(Exception):
                loop.remove_signal_handler(sig)
        lock.release()


def run(argv: list[str] | None = None) -> None:
    """`swarmd [--config PATH]` (launchd: com.swarm.swarmd)."""
    p = argparse.ArgumentParser(prog="swarmd", description="swarm daemon")
    p.add_argument("--config", type=Path, default=None, help=f"default {DEFAULT_CONFIG}")
    a = p.parse_args(argv)
    logging.basicConfig(
        level=os.environ.get("SWARM_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    rc = asyncio.run(main(a.config or default_config_path()))
    if rc in (EXIT_CONFIG, EXIT_LOCKED) and os.environ.get("XPC_SERVICE_NAME") == LAUNCHD_LABEL:
        wallclock.sleep(LAUNCHD_RETRY_PAUSE_S)  # KeepAlive would respawn us every 10 s
    sys.exit(rc)


if __name__ == "__main__":
    run()
