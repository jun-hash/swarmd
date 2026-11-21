"""swarmd assembly: single instance, boot recovery, heartbeat, ordered and bounded
shutdown, auth monitor, daily prune. Adapters are fakes (no Chrome, claude, Telegram, Hermes)."""

import asyncio
import json
import os
import plistlib
import signal
import sqlite3
import subprocess
import sys
import time as wall
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest
from conftest import GENERAL, SECRET
from fakes import FakeTelegram
from workers.conftest import BLOCK, FakeBrowser, FakeGuard, FakeHarness, FakeRegistry

from swarm import daemon as D
from swarm import housekeeping as H
from swarm import watchdog as W
from swarm.browser.guards import GuardCache
from swarm.clock import SEOUL, FakeClock, zone_of
from swarm.engine.fake import FakeEngine
from swarm.projects.store import SqliteProjectStore
from swarm.scheduler.ledger import SqliteQuotaLedger
from swarm.store.blobs import BlobStore
from swarm.store.db import Database
from swarm.store.events import SqliteEventLog
from swarm.types import BacklogItem, Project, Variant

CH = "telegram:-100123:42"
GB = 1024**3


async def ws_down(url, headers):
    raise OSError("hermes down")


class Stack:
    """Seams for one daemon start, keeping the fakes reachable from the test."""

    def __init__(self, **kw) -> None:
        self.trace: list[str] = []
        self.harness = FakeHarness(self.trace)
        self.browser = FakeBrowser(self.trace)
        self.guard = FakeGuard(self.trace)
        self.tg = FakeTelegram()
        self.auth_ok = True
        self.started = asyncio.Event()
        self.daemon: D.Daemon | None = None

        async def guards(browser_id, profile):
            return self.guard

        async def auth_check():
            return self.auth_ok

        def on_started(d):
            self.daemon = d
            self.started.set()

        base = dict(
            engine=FakeEngine([]),
            browser=self.browser,
            guards=guards,
            harnesses=FakeRegistry(self.harness),
            telegram_transport=self.tg.transport,
            hermes_transport=httpx.MockTransport(lambda r: httpx.Response(503)),
            ws_connect=ws_down,
            auth_check=auth_check,
            free_bytes=lambda: 100 * GB,
            stop=asyncio.Event(),
            on_started=on_started,
            tick_every_s=0.05,
        )
        base.update(kw)
        self.seams = D.Seams(**base)


class Running:
    def __init__(self, stack: Stack, task: asyncio.Task) -> None:
        self.stack = stack
        self.task = task

    @property
    def daemon(self) -> D.Daemon:
        assert self.stack.daemon is not None
        return self.stack.daemon

    async def stop(self) -> int:
        self.stack.seams.stop.set()
        return await asyncio.wait_for(self.task, 40)


async def start_daemon(tmp_home, stack: Stack | None = None) -> Running:
    stack = stack or Stack()
    task = asyncio.create_task(D.main(tmp_home.config, seams=stack.seams))
    started = asyncio.create_task(stack.started.wait())
    done, _ = await asyncio.wait({task, started}, timeout=15, return_when="FIRST_COMPLETED")
    started.cancel()
    assert stack.started.is_set(), f"daemon did not start: {task.result() if done else 'timeout'}"
    r = Running(stack, task)
    await until(lambda: (tmp_home.home / "heartbeat").exists())
    return r


async def run_daemon_once(tmp_home) -> int:
    return await asyncio.wait_for(D.main(tmp_home.config, seams=Stack().seams), 15)


async def until(pred, timeout=10.0):
    end = asyncio.get_running_loop().time() + timeout
    while not pred():
        assert asyncio.get_running_loop().time() < end, "condition not reached"
        await asyncio.sleep(0.02)


def runs_status(tmp_home, run_id: str) -> str:
    c = sqlite3.connect(tmp_home.home / "swarm.db")
    try:
        return c.execute("SELECT status FROM runs WHERE id=?", (run_id,)).fetchone()[0]
    finally:
        c.close()


V = Variant(harness="own", engine="claude", browser="chrome", context_policy="", model=None)


def make_project() -> Project:
    return Project(
        id="prj_compet", slug="compet", name="경쟁사",
        goal={"objective": "경쟁사 변화를 먼저 안다", "key_results": []},
        strategy="", policies=[], autonomy=2,
        memory={"context": "", "learnings": [], "failed_attempts": []},
        backlog=[], artifacts=[], channel=CH, board="compet", default_profile="work",
        default_variant=V, weight=2, watches=[],
    )  # fmt: skip


def seed_project_with_ready_item(tmp_home) -> None:
    clock = FakeClock(datetime.now(SEOUL))
    db = Database(tmp_home.home / "swarm.db")
    log = SqliteEventLog(db, BlobStore(tmp_home.home / "blobs", clock))
    projects = SqliteProjectStore(db, log, tmp_home.home, clock)
    pid = projects.create(make_project())
    projects.add_backlog(
        pid,
        BacklogItem(title="C사 가격표", intent="C사 가격표를 정리한다", done_criteria=["표"],
                    status="ready", priority=0, value=4),
    )  # fmt: skip
    db.close()


# --- single instance ----------------------------------------------------------------------


async def test_second_instance_exits_3(tmp_home):
    p1 = await start_daemon(tmp_home)
    rc = await run_daemon_once(tmp_home)
    assert rc == 3
    assert not p1.task.done()  # the first one is untouched
    assert await p1.stop() == 0


def test_second_process_exits_3_before_touching_the_db(tmp_home):
    lock = D.InstanceLock(tmp_home.home / "swarmd.lock")
    assert lock.acquire()
    try:
        p = subprocess.run(
            [sys.executable, "-m", "swarm.daemon", "--config", str(tmp_home.config)],
            capture_output=True, text=True, timeout=60,
        )  # fmt: skip
    finally:
        lock.release()
    assert p.returncode == 3, p.stderr
    assert "another swarmd" in p.stderr
    assert not (tmp_home.home / "swarm.db").exists()


async def test_lock_is_released_after_shutdown(tmp_home):
    p1 = await start_daemon(tmp_home)
    assert await p1.stop() == 0
    lock = D.InstanceLock(tmp_home.home / "swarmd.lock")
    assert lock.acquire()
    lock.release()


# --- config fail-fast -----------------------------------------------------------------------


async def test_missing_general_channel_fails_fast(tmp_home, capsys):
    tmp_home.write_config(general_channel="")
    assert await run_daemon_once(tmp_home) == 2
    err = capsys.readouterr().err
    assert "general_channel" in err and "General" in err
    assert not (tmp_home.home / "swarm.db").exists()


async def test_bad_general_channel_and_missing_secret_fail_fast(tmp_home, capsys):
    tmp_home.write_config(general_channel="slack:#x")
    assert await run_daemon_once(tmp_home) == 2
    tmp_home.write_config()
    (tmp_home.home / "secret").unlink()
    assert await run_daemon_once(tmp_home) == 2
    assert "swarm init" in capsys.readouterr().err


async def test_unknown_timezone_fails_fast_as_a_config_error(tmp_home, capsys):
    tmp_home.write_config(timezone="Asia/Seol")
    assert await run_daemon_once(tmp_home) == 2
    err = capsys.readouterr().err
    assert "timezone" in err and "'Asia/Seol'" in err
    assert not (tmp_home.home / "swarm.db").exists()


async def test_notifier_gets_the_general_channel(tmp_home):
    p = await start_daemon(tmp_home)
    try:
        assert p.daemon.notifier._general == GENERAL  # the Ask fallback is on
    finally:
        await p.stop()


# --- boot recovery + heartbeat ----------------------------------------------------------------


@pytest.fixture
def seeded_running_run(tmp_home):
    db = Database(tmp_home.home / "swarm.db")
    D.WorkerPool(
        db=db,
        projects=None,
        inbox=None,
        ledger=None,
        engines={},
        compatible=(),
        browsers={},
        guards=None,
        harnesses=None,
        log=None,
        clock=None,
        output_root=tmp_home.home / "runs",
    )  # the constructor only migrates
    db.execute(
        "INSERT INTO runs(id, order_id, project_id, order_json, variant_json, status, started_at)"
        " VALUES ('run_old', 'ord_old', NULL, '{}', '{}', 'running', '2026-11-06T09:00:00+09:00')"
    )
    db.close()
    return "run_old"


async def test_boot_recovery_interrupts_running_runs(tmp_home, seeded_running_run):
    p = await start_daemon(tmp_home)
    try:
        assert runs_status(tmp_home, seeded_running_run) == "interrupted"
        t = p.daemon.trace
        assert t.index("recover_on_boot") < t.index("start:agenda")
        assert t[-1] == "start:agenda"  # the agenda loop starts last
    finally:
        await p.stop()


async def test_heartbeat_is_touched_after_each_tick(tmp_home):
    p = await start_daemon(tmp_home)
    try:
        hb = tmp_home.home / "heartbeat"
        os.utime(hb, (0, 0))
        await until(lambda: hb.stat().st_mtime > 0)
        assert abs(hb.stat().st_mtime - wall.time()) < 60
    finally:
        await p.stop()


# --- SIGTERM leaves runs interrupted -------------------------------------------------------------


async def test_sigterm_leaves_runs_interrupted(tmp_home):
    seed_project_with_ready_item(tmp_home)
    stack = Stack()
    stack.harness.script = [BLOCK]
    p = await start_daemon(tmp_home, stack)
    await asyncio.wait_for(stack.harness.started.wait(), 10)
    [run] = p.daemon.pool.running()
    assert run["status"] == "running"
    assert signal.getsignal(signal.SIGTERM) not in (signal.SIG_DFL, None)  # ours, not default
    os.kill(os.getpid(), signal.SIGTERM)
    assert await asyncio.wait_for(p.task, 40) == 0
    assert runs_status(tmp_home, run["id"]) == "interrupted"
    assert stack.browser.closed == ["bs_1"] and stack.guard.disarmed == ["bs_1"]
    assert signal.getsignal(signal.SIGTERM) is signal.SIG_DFL  # handler removed again


# --- shutdown order + bound --------------------------------------------------------------------


async def test_shutdown_runs_in_order(tmp_home):
    p = await start_daemon(tmp_home)
    d = p.daemon
    seen: dict[str, bool] = {}
    real = d.mirror.aclose

    async def mirror_aclose():
        seen["mirror_run_done"] = d.seams.stop.is_set() and all(
            "mirror" not in t.get_name() or t.done() for t in asyncio.all_tasks()
        )
        await real()

    d.mirror.aclose = mirror_aclose
    assert await p.stop() == 0
    order = d.trace[d.trace.index("pool.interrupt") :]
    assert order == [
        "pool.interrupt", "loops", "mirror.aclose", "coordinator.aclose", "pool.aclose",
        "front.aclose", "api.drain", "notifier.flush", "notifier.aclose", "http", "browser",
        "db.close",
    ]  # fmt: skip
    assert seen["mirror_run_done"]  # mirror.run (and its loops) ended before aclose


async def test_shutdown_is_bounded_when_a_step_hangs(tmp_home):
    stack = Stack(shutdown_budget_s=3.0, shutdown_step_cap_s=0.5)
    p = await start_daemon(tmp_home, stack)
    d = p.daemon
    closed: list[str] = []

    async def hang():
        await asyncio.Event().wait()

    real = d.coordinator.aclose

    async def coord_aclose():
        closed.append("coordinator")
        await real()

    d.mirror.aclose = hang
    d.coordinator.aclose = coord_aclose
    t0 = wall.monotonic()
    assert await p.stop() == 0
    assert wall.monotonic() - t0 < 3.5
    assert closed == ["coordinator"]  # the hung step did not eat the rest
    lock = D.InstanceLock(tmp_home.home / "swarmd.lock")
    assert lock.acquire()  # released even so
    lock.release()


class StubbornServer:
    """A uvicorn stand-in that ignores `should_exit`: the http step can only end by its cap."""

    started = True
    should_exit = False

    async def serve(self) -> None:
        await asyncio.Event().wait()


class TimedTrace(list):
    """A daemon trace that also records when each entry was appended (wall monotonic)."""

    def __init__(self, items) -> None:
        super().__init__(items)
        self.at: list[float | None] = [None] * len(items)

    def append(self, name) -> None:
        super().append(name)
        self.at.append(wall.monotonic())

    def durations(self, start: str) -> dict[str, float]:
        """Seconds from each entry, `start` onward, to the entry after it."""
        i = self.index(start)
        return {self[k]: self.at[k + 1] - self.at[k] for k in range(i, len(self) - 1)}


async def test_every_shutdown_step_is_bounded_by_its_cap(tmp_home, monkeypatch):
    async def hang(*_a, **_kw):
        await asyncio.Event().wait()

    monkeypatch.setattr(D, "make_server", lambda app, port: StubbornServer())
    monkeypatch.setattr(D, "FLUSH_CAP_S", 1.0)  # the last flush has its own bound, not the cap
    cap = 0.4  # wide enough that scheduling stalls stay inside the per-step slack below
    stack = Stack(shutdown_budget_s=20.0, shutdown_step_cap_s=cap, auth_check=hang)  # loops hang
    p = await start_daemon(tmp_home, stack)
    d = p.daemon
    for obj, name in [(d.pool, "aclose"), (d.mirror, "aclose"), (d.coordinator, "aclose"),
                      (d.front, "aclose"), (d.api, "drain"), (d.notifier, "drain_once"),
                      (d.notifier, "aclose"), (d.browser, "shutdown")]:  # fmt: skip
        setattr(obj, name, hang)
    d.trace = TimedTrace(d.trace)
    t0 = wall.monotonic()
    assert await p.stop() == 0
    elapsed = wall.monotonic() - t0
    order = d.trace[d.trace.index("pool.interrupt") :]
    assert order == [
        "pool.interrupt", "loops", "mirror.aclose", "coordinator.aclose", "pool.aclose",
        "front.aclose", "api.drain", "notifier.flush", "notifier.aclose", "http", "browser",
        "db.close",
    ]  # fmt: skip
    bound = 10 * cap + D.FLUSH_CAP_S  # 10 steps capped at `cap`, the flush at FLUSH_CAP_S
    assert bound <= elapsed < bound + 2.0  # every step ran to its own bound, no longer
    took = d.trace.durations("pool.interrupt")
    limits = {name: cap for name in order[:-1]} | {"notifier.flush": D.FLUSH_CAP_S}
    for name, limit in limits.items():  # slack under one bound: a doubled bound fails
        assert limit - 0.01 <= took[name] < 1.75 * limit, (name, took[name])
    lock = D.InstanceLock(tmp_home.home / "swarmd.lock")
    assert lock.acquire()
    lock.release()


# --- internal API on the daemon --------------------------------------------------------------


async def test_cancel_route_is_served_with_the_bearer(tmp_home):
    p = await start_daemon(tmp_home)
    try:
        url = f"http://127.0.0.1:{tmp_home.port}/internal/runs/run_nope/cancel"
        async with httpx.AsyncClient(timeout=5) as c:
            r = await c.post(url, json={"reason": "stuck"})
            assert r.status_code == 401
            r = await c.post(
                url, json={"reason": "stuck"}, headers={"Authorization": f"Bearer {SECRET}"}
            )
            assert r.status_code == 404
            assert (await c.get(f"http://127.0.0.1:{tmp_home.port}/internal/health")).json()
    finally:
        await p.stop()


async def test_mirror_alert_goes_to_the_project_topic_or_general(tmp_home):
    seed_project_with_ready_item(tmp_home)
    p = await start_daemon(tmp_home)
    try:
        await p.daemon.mirror_alert("prj_compet", "칸반 FYI")
        await p.daemon.mirror_alert(None, "칸반 전체 FYI")
        rows = p.daemon.db.execute("SELECT channel, text FROM outbox ORDER BY seq").fetchall()
        got = [(r["channel"], r["text"]) for r in rows if "칸반" in r["text"]]
        assert got == [(CH, "칸반 FYI"), (GENERAL, "칸반 전체 FYI")]
    finally:
        await p.stop()


# --- auth monitor ---------------------------------------------------------------------------------


class SpyFyi:
    def __init__(self) -> None:
        self.sent: list[tuple[str, str, bool]] = []

    async def __call__(self, channel, text, urgent=False):
        self.sent.append((channel, text, urgent))


@pytest.fixture
def auth_parts(tmp_path):
    clock = FakeClock(datetime(2026, 11, 7, 10, 0, tzinfo=SEOUL))
    db = Database(tmp_path / "a.db")
    log = SqliteEventLog(db, BlobStore(tmp_path / "blobs", clock))
    ledger = SqliteQuotaLedger(db, log, clock, instance_id="inst_a")
    return db, log, ledger, clock


async def test_auth_failure_stops_leases_and_alerts_general_once(auth_parts):
    db, log, ledger, clock = auth_parts
    state = {"ok": False}

    async def check():
        return state["ok"]

    fyi = SpyFyi()
    mon = H.AuthMonitor(ledger=ledger, kv=D.daemon_kv(db), check=check, fyi=fyi, general=GENERAL,
                        engine_id="claude", clock=clock)  # fmt: skip
    assert await mon.check_once() is False
    assert ledger.disabled("claude")
    assert ledger.lease(engine_pref=["claude"], purpose="advance") is None  # "막히면 쉰다"
    await mon.check_once()
    await mon.sync_alert()
    mon2 = H.AuthMonitor(ledger=ledger, kv=D.daemon_kv(db), check=check, fyi=fyi, general=GENERAL,
                        engine_id="claude", clock=clock)  # fmt: skip
    await mon2.check_once()
    assert [(c, u) for c, _, u in fyi.sent] == [(GENERAL, True)]  # one urgent FYI
    assert "로그인" in fyi.sent[0][1]
    state["ok"] = True
    await mon2.check_once()
    assert not ledger.disabled("claude")
    assert ledger.lease(engine_pref=["claude"], purpose="advance") is not None
    assert len(fyi.sent) == 2 and fyi.sent[1][2] is False  # "돌아왔어요", not urgent
    kinds = [e["kind"] for e in log.query(kinds=["engine.auth_failed", "engine.auth_ok"])]
    assert kinds == ["engine.auth_failed", "engine.auth_ok"]


async def test_auth_failure_seen_by_the_engine_is_alerted_by_the_poll(auth_parts):
    from swarm.types import UsageSignal

    db, _, ledger, clock = auth_parts

    async def check():
        return True

    fyi = SpyFyi()
    mon = H.AuthMonitor(ledger=ledger, kv=D.daemon_kv(db), check=check, fyi=fyi, general=GENERAL,
                        engine_id="claude", clock=clock)  # fmt: skip
    ledger.observe(
        engine="claude", signal=UsageSignal(kind="auth_failed", reset_at=None, at=clock.now())
    )  # a run hit "please log in"
    await mon.sync_alert()
    await mon.sync_alert()
    assert len(fyi.sent) == 1 and fyi.sent[0][2] is True


async def test_auth_check_crash_is_not_a_logout(auth_parts):
    db, _, ledger, clock = auth_parts

    async def check():
        raise RuntimeError("no claude binary")

    fyi = SpyFyi()
    mon = H.AuthMonitor(ledger=ledger, kv=D.daemon_kv(db), check=check, fyi=fyi, general=GENERAL,
                        engine_id="claude", clock=clock)  # fmt: skip
    assert await mon.check_once() is True
    assert not ledger.disabled("claude") and fyi.sent == []


async def test_default_auth_check_strips_api_keys_and_never_bare(monkeypatch):
    import swarm.engine.claude as C

    seen: dict = {}

    class Proc:
        returncode = 0
        pid = 1

        async def communicate(self):
            return b'{"loggedIn": true}', b""

    async def fake_exec(binary, *args, **kw):
        seen.update(binary=binary, args=args, env=kw["env"])
        return Proc()

    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-should-not-leak")
    monkeypatch.setenv("ANTHROPIC_AUTH_TOKEN", "tok-should-not-leak")
    monkeypatch.setattr(C.asyncio, "create_subprocess_exec", fake_exec)
    assert await H.default_auth_check("claude") is True
    assert seen["args"] == ("auth", "status") and "--bare" not in seen["args"]
    assert "ANTHROPIC_API_KEY" not in seen["env"] and "ANTHROPIC_AUTH_TOKEN" not in seen["env"]


# --- daily prune ----------------------------------------------------------------------------


@pytest.fixture
def janitor_parts(tmp_path):
    clock = FakeClock(datetime(2026, 11, 7, 2, 59, tzinfo=SEOUL))
    db = Database(tmp_path / "j.db")
    blobs = BlobStore(tmp_path / "blobs", clock)
    shots = tmp_path / "blobs" / "shots"
    shots.mkdir()
    return db, blobs, shots, clock


def _shot(shots, name, when: datetime):
    p = shots / name
    p.write_bytes(b"png")
    os.utime(p, (when.timestamp(), when.timestamp()))
    return p


async def test_daily_prune_switches_to_7_days_when_low_disk(janitor_parts):
    db, blobs, shots, clock = janitor_parts
    free = {"b": 100 * GB}
    fyi = SpyFyi()
    j = H.BlobJanitor(blobs=blobs, shots_dir=shots, kv=D.daemon_kv(db), fyi=fyi, general=GENERAL,
                      clock=clock, free_bytes=lambda: free["b"])  # fmt: skip
    old = blobs.put(b"x", "png")  # 31 days old by prune time
    old_shot = _shot(shots, "shot_1_a.png", clock.now())
    clock.advance(timedelta(days=23))
    week = blobs.put(b"y", "png")  # 8 days old by prune time
    week_shot = _shot(shots, "shot_2_b.png", clock.now())
    clock.advance(timedelta(days=8, minutes=1))  # 03:00 + 8 days later
    assert j.due(clock.now())
    assert await j.prune_once(clock.now()) == (30, 2)
    assert not blobs.path(old).exists() and not old_shot.exists()
    assert blobs.path(week).exists() and week_shot.exists()
    assert fyi.sent == [] and not j.due(clock.now())  # once a day
    clock.advance(timedelta(days=1))
    free["b"] = 4 * GB
    assert j.due(clock.now())
    keep, removed = await j.prune_once(clock.now())
    assert (keep, removed) == (7, 2)
    assert not blobs.path(week).exists() and not week_shot.exists()
    [(ch, text, urgent)] = fyi.sent
    assert ch == GENERAL and "5GB" in text and "7일" in text and urgent is False


async def test_prune_waits_for_3am(janitor_parts):
    db, blobs, shots, clock = janitor_parts
    j = H.BlobJanitor(
        blobs=blobs, shots_dir=shots, kv=D.daemon_kv(db), fyi=SpyFyi(), general=GENERAL,
        clock=clock, free_bytes=lambda: 100 * GB,
    )  # fmt: skip
    assert not j.due(clock.now())  # 02:59
    clock.advance(timedelta(minutes=1))
    assert j.due(clock.now())


# --- launchd units -----------------------------------------------------------------------------

OPS = Path(__file__).resolve().parents[1] / "ops" / "launchd"


def test_plists_are_valid():
    found = sorted(OPS.glob("*.plist"))
    assert [p.name for p in found] == ["com.swarm.swarmd.plist", "com.swarm.watchdog.plist"]
    for p in found:
        plistlib.loads(p.read_bytes())


def test_plists_match_the_process_contract():
    d = plistlib.loads((OPS / "com.swarm.swarmd.plist").read_bytes())
    assert d["Label"] == "com.swarm.swarmd" == W.LABEL
    assert d["KeepAlive"] is True and d["RunAtLoad"] is True
    assert d["ExitTimeOut"] > D.SHUTDOWN_BUDGET_S  # launchd never SIGKILLs a clean shutdown
    assert d["ProgramArguments"][0].endswith("/bin/swarmd")
    for key in ("StandardOutPath", "StandardErrorPath"):
        assert d[key].startswith("__HOME__/swarm/logs/")
    w = plistlib.loads((OPS / "com.swarm.watchdog.plist").read_bytes())
    assert w["Label"] == "com.swarm.watchdog" and w["StartInterval"] == 120
    assert w["ProgramArguments"][1:3] == ["-m", "swarm.watchdog"]
    assert w["StandardErrorPath"].startswith("__HOME__/swarm/logs/")
    for unit in (d, w):
        env = unit["EnvironmentVariables"]
        assert "ANTHROPIC_API_KEY" not in env and "__HOME__/.local/bin" in env["PATH"]


# --- guard cache (a dead guard is replaced) -----------------------------------------------------


class FakeChromeForGuards:
    def __init__(self) -> None:
        self.opened: list = []

    async def open_guard(self, profile, allowlist):
        g = FakeGuard([])
        g.connected = True
        g.closed = False

        async def close():
            g.closed = True
            raise RuntimeError("socket already gone")  # closing a dead guard may fail

        g.close = close
        self.opened.append((profile, list(allowlist), g))
        return g


async def test_guard_cache_replaces_a_disconnected_guard():
    chrome = FakeChromeForGuards()
    cache = GuardCache(chrome, ("example.com",))
    g1 = await cache("chrome", "work")
    assert await cache("chrome", "work") is g1  # one guard per profile while it is alive
    assert await cache("chrome", "other") is not g1
    g1.connected = False  # Chrome crashed and was relaunched: the guard's CDP is gone
    g2 = await cache("chrome", "work")
    assert g2 is not g1 and g2.connected and g1.closed
    s = {"id": "bs_9", "guard": "off"}
    await g2.arm(s, order_id="wo_1", mode="guarded")  # the fresh guard works
    assert s["guard"] == "guarded"
    assert [p for p, _, _ in chrome.opened] == ["work", "other", "work"]
    assert all(a == ["example.com"] for _, a, _ in chrome.opened)
    await cache.aclose()


class SlowChromeForGuards(FakeChromeForGuards):
    async def open_guard(self, profile, allowlist):
        await asyncio.sleep(0.01)  # a real CDP connect yields: concurrent callers interleave
        return await super().open_guard(profile, allowlist)


async def test_guard_cache_shares_one_guard_across_concurrent_opens_and_reopens():
    chrome = SlowChromeForGuards()
    cache = GuardCache(chrome, ("example.com",))
    first = await asyncio.gather(*(cache("chrome", "work") for _ in range(3)))
    assert len({id(g) for g in first}) == 1 and len(chrome.opened) == 1
    first[0].connected = False
    again = await asyncio.gather(*(cache("chrome", "work") for _ in range(3)))
    assert len({id(g) for g in again}) == 1 and again[0] is not first[0]
    assert len(chrome.opened) == 2 and first[0].closed  # one reopen, under the profile lock
    await cache.aclose()


# --- watchdog.restart in the event log -----------------------------------------------------------


def restart_events(tmp_home) -> list[dict]:
    c = sqlite3.connect(tmp_home.home / "swarm.db")
    try:
        rows = c.execute(
            "SELECT payload_json FROM events WHERE kind='watchdog.restart' ORDER BY seq"
        )
        return [json.loads(r[0]) for r in rows]
    finally:
        c.close()


def record_restart(tmp_home, at: str) -> None:
    p = tmp_home.home / "watchdog.json"
    st = W.WatchdogState.load(p)
    st.day, st.restarts, st.last_restart_at = at[:10], st.restarts + 1, at
    st.history.append(at)
    st.save(p)


async def test_watchdog_restarts_are_logged_once_each_at_boot(tmp_home):
    record_restart(tmp_home, "2026-11-07T09:00:00+09:00")
    record_restart(tmp_home, "2026-11-07T09:30:00+09:00")
    p = await start_daemon(tmp_home)
    assert await p.stop() == 0
    assert [e["at"] for e in restart_events(tmp_home)] == [
        "2026-11-07T09:00:00+09:00", "2026-11-07T09:30:00+09:00",
    ]  # fmt: skip
    p = await start_daemon(tmp_home)  # same watchdog.json: nothing new
    assert await p.stop() == 0
    assert len(restart_events(tmp_home)) == 2
    record_restart(tmp_home, "2026-11-07T11:00:00+09:00")
    p = await start_daemon(tmp_home)
    assert await p.stop() == 0
    got = restart_events(tmp_home)
    assert len(got) == 3 and got[-1]["at"] == "2026-11-07T11:00:00+09:00"
    assert got[-1]["label"] == "com.swarm.swarmd"


async def test_old_watchdog_json_without_history_still_logs_the_last_restart(tmp_home):
    (tmp_home.home / "watchdog.json").write_text(
        json.dumps({"day": "2026-11-07", "restarts": 1,
                    "last_restart_at": "2026-11-07T09:00:00+09:00"})
    )  # fmt: skip
    p = await start_daemon(tmp_home)
    assert await p.stop() == 0
    assert [e["at"] for e in restart_events(tmp_home)] == ["2026-11-07T09:00:00+09:00"]


@pytest.mark.parametrize("body", ["[1, 2]", '"x"', "{not json", '{"history": [5]}', "null"])
async def test_malformed_watchdog_json_never_blocks_boot(tmp_home, body):
    (tmp_home.home / "watchdog.json").write_text(body)
    p = await start_daemon(tmp_home)
    assert await p.stop() == 0
    assert restart_events(tmp_home) == []


# --- shutdown backstop and lock edge cases -------------------------------------------------------


async def test_shutdown_backstop_forces_exit_when_a_step_ignores_cancellation(tmp_home):
    released = asyncio.Event()
    forced: list[str] = []

    def force_exit():
        forced.append("exit")
        released.set()  # the real backstop is os._exit: the kernel drops the lock

    stack = Stack(shutdown_budget_s=0.5, shutdown_step_cap_s=0.2, backstop_grace_s=0.3,
                  force_exit=force_exit)  # fmt: skip
    p = await start_daemon(tmp_home, stack)

    async def stubborn():  # swallows its cancellation: wait_for cannot bound it
        while not released.is_set():
            try:
                await released.wait()
            except asyncio.CancelledError:
                continue

    p.daemon.mirror.aclose = stubborn
    assert await p.stop() == 0
    assert forced == ["exit"]


async def test_backstop_is_disarmed_after_a_clean_shutdown(tmp_home):
    forced: list[str] = []
    stack = Stack(shutdown_budget_s=0.5, backstop_grace_s=0.2, force_exit=lambda: forced.append(1))
    p = await start_daemon(tmp_home, stack)
    assert await p.stop() == 0
    await asyncio.sleep(1.0)
    assert forced == []


def test_lock_survives_sigkill_of_its_holder(tmp_home):
    code = (
        "import sys, time; from pathlib import Path; from swarm.daemon import InstanceLock;"
        "assert InstanceLock(Path(sys.argv[1])).acquire(); print('held', flush=True);"
        "time.sleep(60)"
    )
    path = tmp_home.home / "swarmd.lock"
    p = subprocess.Popen([sys.executable, "-c", code, str(path)], stdout=subprocess.PIPE, text=True)
    try:
        assert p.stdout.readline().strip() == "held"
        assert not D.InstanceLock(path).acquire()
        p.kill()  # SIGKILL: no finally, no release
        p.wait(10)
        lock = D.InstanceLock(path)
        assert lock.acquire()  # the kernel dropped it
        lock.release()
    finally:
        if p.poll() is None:
            p.kill()


def test_launchd_respawn_of_a_locked_or_misconfigured_daemon_waits(tmp_home, monkeypatch):
    slept: list[float] = []
    monkeypatch.setattr(D.wallclock, "sleep", lambda s: slept.append(s))
    tmp_home.write_config(general_channel="")
    monkeypatch.delenv("XPC_SERVICE_NAME", raising=False)
    with pytest.raises(SystemExit) as e:
        D.run(["--config", str(tmp_home.config)])
    assert e.value.code == 2 and slept == []  # by hand: exit at once
    monkeypatch.setenv("XPC_SERVICE_NAME", "com.swarm.swarmd")
    with pytest.raises(SystemExit) as e:
        D.run(["--config", str(tmp_home.config)])
    assert e.value.code == 2 and slept == [D.LAUNCHD_RETRY_PAUSE_S]  # no 10 s respawn flood


def test_launchd_respawn_with_an_unknown_timezone_waits(tmp_home, monkeypatch):
    slept: list[float] = []
    monkeypatch.setattr(D.wallclock, "sleep", lambda s: slept.append(s))
    tmp_home.write_config(timezone="Asia/Seol")
    monkeypatch.setenv("XPC_SERVICE_NAME", "com.swarm.swarmd")
    with pytest.raises(SystemExit) as e:
        D.run(["--config", str(tmp_home.config)])
    assert e.value.code == 2 and slept == [D.LAUNCHD_RETRY_PAUSE_S]


# --- SWARM_SIMULATE_LIMIT (soak hook) ---------------------------------------------------------


@pytest.mark.parametrize("value, wrapped", [(None, False), ("1", True)])
def test_simulate_limit_env_wraps_the_engine_only_when_set(tmp_home, monkeypatch, value, wrapped):
    from swarm.engine.simulate import ENV, LimitSimulatingEngine

    if value is None:
        monkeypatch.delenv(ENV, raising=False)
    else:
        monkeypatch.setenv(ENV, value)
    fake = FakeEngine([])
    d = D.Daemon(D.load_and_validate(tmp_home.config), SECRET, Stack(engine=fake).seams)
    try:
        assert isinstance(d.engine, LimitSimulatingEngine) is wrapped
        assert (d.engine is fake) is not wrapped
    finally:
        d.db.close()


def test_default_variant_is_own_claude_with_the_configured_model(tmp_home):
    cfg = D.load_and_validate(tmp_home.config)
    v = D.default_variant(cfg, "chrome")
    assert (v["harness"], v["engine"], v["browser"]) == ("own", "claude", "chrome")
    assert v["context_policy"] == "rollover_brief" and v["model"] == cfg.claude_model


def test_configured_timezone_reaches_the_clock_the_log_and_the_alert_window(tmp_home):
    tmp_home.write_config(timezone="America/New_York")
    d = D.Daemon(D.load_and_validate(tmp_home.config), SECRET, Stack().seams)
    try:
        assert str(zone_of(d.clock)) == "America/New_York"
        d._emit("daemon.boot")
        assert str(next(d.log.query(kinds=["daemon.boot"]))["ts"].tzinfo) == "America/New_York"
        # 12:00 UTC is 07:00 in New York: before the 08:00 alert window opens
        assert not d.notifier._window.is_open(datetime(2026, 11, 5, 12, 0, tzinfo=UTC))
        assert d.notifier._window.is_open(datetime(2026, 11, 5, 14, 0, tzinfo=UTC))
    finally:
        d.db.close()
