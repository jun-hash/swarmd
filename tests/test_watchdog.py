"""Watchdog: heartbeat restarts via launchctl, restart FYI, stuck-run cancel. All
side effects are fakes; the DB is read the way the real watchdog reads it (read-only)."""

import json
import logging
import os
from datetime import datetime, timedelta

import httpx
import pytest
from conftest import GENERAL, SECRET, TOKEN

from swarm import watchdog as W
from swarm.clock import SEOUL, FakeClock
from swarm.config import load_config
from swarm.store.blobs import BlobStore
from swarm.store.db import Database
from swarm.store.events import SqliteEventLog
from swarm.workers.pool import WorkerPool


class FakeLaunchctl:
    def __init__(self) -> None:
        self.calls = 0
        self.ok = True

    def __call__(self) -> bool:
        self.calls += 1
        return self.ok


class Recorder:
    def __init__(self, ok: bool = True) -> None:
        self.calls: list = []
        self.ok = ok

    def __call__(self, *args) -> bool:
        self.calls.append(args)
        return self.ok


# Overrides the root clock: the restart-budget tests assert the day is 2026-11-07.
@pytest.fixture
def clock():
    return FakeClock(datetime(2026, 11, 7, 10, 0, tzinfo=SEOUL))


@pytest.fixture
def fake_launchctl():
    return FakeLaunchctl()


@pytest.fixture
def fake_internal():
    return Recorder()


@pytest.fixture
def alerts():
    return Recorder()


def make(tmp_home, clock, kick, alert=None, cancel=None) -> W.Watchdog:
    return W.Watchdog(
        home=tmp_home.home, clock=clock, kickstart=kick,
        alert=alert or Recorder(), cancel=cancel or Recorder(),
    )  # fmt: skip


def beat(tmp_home, clock, ago: timedelta) -> None:
    hb = tmp_home.home / "heartbeat"
    hb.touch()
    t = (clock.now() - ago).timestamp()
    os.utime(hb, (t, t))


# --- heartbeat --------------------------------------------------------------------------------


def test_watchdog_restarts_when_heartbeat_stale(fake_launchctl, tmp_home, clock):
    wd = make(tmp_home, clock, fake_launchctl)
    beat(tmp_home, clock, timedelta(minutes=1))
    assert wd.run_once()["restarted"] is False and fake_launchctl.calls == 0
    beat(tmp_home, clock, timedelta(minutes=6))
    r = wd.run_once()
    assert r["restarted"] is True and r["restarts_today"] == 1 and fake_launchctl.calls == 1
    clock.advance(timedelta(minutes=2))  # next pass: still stale, but we just kicked it
    assert wd.run_once()["restarted"] is False and fake_launchctl.calls == 1
    state = json.loads((tmp_home.home / "watchdog.json").read_text())
    assert state["restarts"] == 1 and state["day"] == "2026-11-07"


def test_second_restart_of_the_day_alerts_once(fake_launchctl, tmp_home, clock, alerts):
    wd = make(tmp_home, clock, fake_launchctl, alert=alerts)
    for _ in range(3):
        beat(tmp_home, clock, timedelta(minutes=6))
        assert wd.run_once()["restarted"] is True
        clock.advance(timedelta(minutes=6))
    assert fake_launchctl.calls == 3
    [(text,)] = alerts.calls  # from the 2nd restart of a day, one FYI that day
    assert text.startswith("⚠️ FYI") and "2번" in text
    clock.advance(timedelta(days=1))
    beat(tmp_home, clock, timedelta(minutes=6))
    assert wd.run_once()["restarts_today"] == 1  # a new day starts from zero
    assert len(alerts.calls) == 1


def test_missing_heartbeat_counts_from_first_sight(fake_launchctl, tmp_home, clock):
    wd = make(tmp_home, clock, fake_launchctl)
    assert wd.run_once()["restarted"] is False
    clock.advance(timedelta(minutes=4))
    assert wd.run_once()["restarted"] is False
    clock.advance(timedelta(minutes=2))
    assert wd.run_once()["restarted"] is True and fake_launchctl.calls == 1


def test_failed_kickstart_is_not_a_restart(fake_launchctl, tmp_home, clock, alerts):
    fake_launchctl.ok = False
    wd = make(tmp_home, clock, fake_launchctl, alert=alerts)
    beat(tmp_home, clock, timedelta(minutes=10))
    assert wd.run_once()["restarted"] is False
    clock.advance(timedelta(minutes=2))
    wd.run_once()
    assert fake_launchctl.calls == 2  # retried at the next pass (no grace for a failed kick)
    assert json.loads((tmp_home.home / "watchdog.json").read_text())["restarts"] == 0


def test_kickstart_command(monkeypatch):
    seen = {}

    class P:
        returncode = 0
        stderr = ""

    def fake_run(cmd, **kw):
        seen["cmd"] = cmd
        return P()

    monkeypatch.setattr(W.subprocess, "run", fake_run)
    assert W.launchctl_kickstart() is True
    assert seen["cmd"] == ["launchctl", "kickstart", "-k", f"gui/{os.getuid()}/com.swarm.swarmd"]


# --- stuck runs -------------------------------------------------------------------------------


class Seeder:
    """Writes runs + events the way swarmd does (pool migrations, SqliteEventLog)."""

    def __init__(self, tmp_home, clock) -> None:
        self.clock = clock
        self.db = Database(tmp_home.home / "swarm.db")
        self.log = SqliteEventLog(self.db, BlobStore(tmp_home.home / "blobs", clock))
        WorkerPool(
            db=self.db, projects=None, inbox=None, ledger=None, engines={}, compatible=(),
            browsers={}, guards=None, harnesses=None, log=self.log, clock=clock,
            output_root=tmp_home.home / "runs",
        )  # fmt: skip

    def run(self, rid: str, status: str = "running") -> None:
        self.db.execute(
            "INSERT INTO runs(id, order_id, project_id, order_json, variant_json, status,"
            " started_at) VALUES (?, 'ord', NULL, '{}', '{}', ?, ?)",
            (rid, status, self.clock.now().isoformat()),
        )

    def ev(self, rid: str, kind: str, ago: timedelta, **payload) -> None:
        self.log.append(
            {"id": "", "ts": self.clock.now() - ago, "kind": kind, "actor": "workers",
             "project_id": None, "run_id": rid, "payload": payload, "refs": []}
        )  # fmt: skip


def test_watchdog_cancels_stuck_run(fake_internal, fake_launchctl, tmp_home, clock):
    beat(tmp_home, clock, timedelta(seconds=10))  # the daemon is alive
    s = Seeder(tmp_home, clock)
    m = timedelta(minutes=1)
    s.run("run_quiet")  # no tool_result for 16 minutes
    s.ev("run_quiet", "run.running", 30 * m)
    s.ev("run_quiet", "worker.tool_result", 16 * m, id="t", ok=True, preview="", ms=1)
    s.run("run_busy")
    s.ev("run_busy", "run.running", 30 * m)
    s.ev("run_busy", "worker.tool_result", 1 * m, id="t", ok=True, preview="", ms=1)
    s.run("run_loop")  # the same click six times in a row
    s.ev("run_loop", "run.running", 5 * m)
    for _ in range(6):
        s.ev("run_loop", "worker.tool_call", m, id="c", name="browser_click", args={"ref": "e1"})
        s.ev("run_loop", "worker.tool_result", m, id="c", ok=True, preview="", ms=1)
    s.run("run_clicks")  # six clicks on different things: real work
    s.ev("run_clicks", "run.running", 5 * m)
    for i in range(6):
        s.ev("run_clicks", "worker.tool_call", m, id="c", name="browser_click",
             args={"ref": f"e{i}"})  # fmt: skip
    s.run("run_resumed")  # quiet before the park, but just resumed
    s.ev("run_resumed", "worker.tool_result", 60 * m, id="t", ok=True, preview="", ms=1)
    s.ev("run_resumed", "run.running", 2 * m)
    s.run("run_parked", status="waiting_human")
    s.ev("run_parked", "run.running", 90 * m)
    wd = make(tmp_home, clock, fake_launchctl, cancel=fake_internal)
    r = wd.run_once()
    assert sorted(r["cancelled"]) == ["run_loop", "run_quiet"]
    assert sorted(fake_internal.calls) == [("run_loop", "stuck"), ("run_quiet", "stuck")]
    assert fake_launchctl.calls == 0
    clock.advance(timedelta(minutes=2))  # not cancelled again while the cancel is in flight
    wd.run_once()
    assert len(fake_internal.calls) == 2
    clock.advance(timedelta(minutes=10))
    beat(tmp_home, clock, timedelta(seconds=10))
    wd.run_once()
    # still running after 10 more minutes: tried again; run_clicks is now 17 min quiet too
    assert sorted(fake_internal.calls[2:]) == [
        ("run_clicks", "stuck"), ("run_loop", "stuck"), ("run_quiet", "stuck"),
    ]  # fmt: skip


def test_watchdog_never_writes_the_db(fake_internal, fake_launchctl, tmp_home, clock):
    beat(tmp_home, clock, timedelta(seconds=10))
    s = Seeder(tmp_home, clock)
    s.run("run_quiet")
    s.ev("run_quiet", "run.running", timedelta(minutes=20))
    before = s.db.execute("SELECT count(*) FROM events").fetchone()[0]
    make(tmp_home, clock, fake_launchctl, cancel=fake_internal).run_once()
    assert s.db.execute("SELECT count(*) FROM events").fetchone()[0] == before
    assert s.db.execute("SELECT status FROM runs").fetchone()[0] == "running"


def test_no_db_yet_is_fine(fake_internal, fake_launchctl, tmp_home, clock):
    beat(tmp_home, clock, timedelta(seconds=10))
    assert make(tmp_home, clock, fake_launchctl, cancel=fake_internal).run_once()["cancelled"] == []


# --- side-effect clients --------------------------------------------------------------------


def test_cancel_client_posts_reason_with_bearer():
    seen = {}

    def handle(req: httpx.Request) -> httpx.Response:
        seen.update(
            url=str(req.url), auth=req.headers["authorization"], body=json.loads(req.content)
        )
        return httpx.Response(200, json={"ok": True})

    ok = W.cancel_via_internal(8765, SECRET, "run_1", "stuck",
                               transport=httpx.MockTransport(handle))  # fmt: skip
    assert ok and seen["url"] == "http://127.0.0.1:8765/internal/runs/run_1/cancel"
    assert seen["auth"] == f"Bearer {SECRET}" and seen["body"] == {"reason": "stuck"}


def test_telegram_alert_is_plain_text_and_never_logs_the_token(caplog):
    sent = []

    def ok(req: httpx.Request) -> httpx.Response:
        sent.append((req.url.path, json.loads(req.content)))
        return httpx.Response(200, json={"ok": True, "result": {}})

    assert W.send_telegram(
        TOKEN, "telegram:-100123:42", "*hi* <b>", transport=httpx.MockTransport(ok)
    )
    [(path, body)] = sent
    assert path == f"/bot{TOKEN}/sendMessage" and "parse_mode" not in body
    assert body == {"chat_id": "-100123", "message_thread_id": 42, "text": "*hi* <b>"}
    assert W.send_telegram(TOKEN, GENERAL, "x", transport=httpx.MockTransport(ok))
    assert "message_thread_id" not in sent[-1][1]  # thread 1 = General

    def boom(req):
        raise httpx.ConnectError(f"cannot reach {req.url}")

    with caplog.at_level(logging.DEBUG):
        assert not W.send_telegram(TOKEN, GENERAL, "x", transport=httpx.MockTransport(boom))
        denied = httpx.MockTransport(lambda r: httpx.Response(401))
        assert not W.send_telegram(TOKEN, GENERAL, "x", transport=denied)
    assert TOKEN not in caplog.text


def test_main_reads_config_and_runs_one_pass(tmp_home, monkeypatch):
    kicks = []
    monkeypatch.setattr(W, "launchctl_kickstart", lambda: kicks.append(1) or True)
    assert W.main(["--config", str(tmp_home.config)]) == 0  # no heartbeat yet: first sight
    assert (tmp_home.home / "watchdog.json").exists() and kicks == []


def test_main_with_an_unknown_timezone_reports_a_config_error(tmp_home, caplog):
    tmp_home.write_config(timezone="Asia/Seol")
    with caplog.at_level(logging.ERROR):
        assert W.main(["--config", str(tmp_home.config)]) == 2
    assert "watchdog: cannot read config/secret: ZoneInfoNotFoundError" in caplog.text


def test_restart_history_is_kept_for_the_daemon(fake_launchctl, tmp_home, clock):
    wd = make(tmp_home, clock, fake_launchctl)
    for _ in range(2):
        beat(tmp_home, clock, timedelta(minutes=6))
        wd.run_once()
        clock.advance(timedelta(minutes=6))
    st = W.WatchdogState.load(tmp_home.home / "watchdog.json")
    assert len(st.history) == 2 and st.history[-1] == st.last_restart_at


def test_restart_fyi_is_silent_outside_the_alert_window():
    sent = []

    def ok(req: httpx.Request) -> httpx.Response:
        sent.append(json.loads(req.content))
        return httpx.Response(200, json={"ok": True, "result": {}})

    t = httpx.MockTransport(ok)
    assert W.send_telegram(TOKEN, GENERAL, "x", transport=t)
    assert W.send_telegram(TOKEN, GENERAL, "x", silent=True, transport=t)
    assert "disable_notification" not in sent[0] and sent[1]["disable_notification"] is True


def test_main_alert_is_silent_at_night(tmp_home, monkeypatch):
    calls = []
    monkeypatch.setattr(W, "send_telegram", lambda *a, **kw: calls.append(kw) or True)
    night = datetime(2026, 11, 7, 2, 0, tzinfo=SEOUL)
    day = datetime(2026, 11, 7, 10, 0, tzinfo=SEOUL)
    assert W.alert_for(load_config(tmp_home.config), clock=FakeClock(night))("t") is True
    assert W.alert_for(load_config(tmp_home.config), clock=FakeClock(day))("t") is True
    assert [c["silent"] for c in calls] == [True, False]
