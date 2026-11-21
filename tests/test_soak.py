"""scripts/soak.py on a synthetic swarm.db built with the real schemas (no daemon, no Chrome)."""

import importlib.util
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from swarm.clock import SEOUL, FakeClock
from swarm.front import notifier as N
from swarm.projects import store as P
from swarm.store.blobs import BlobStore
from swarm.store.db import Database
from swarm.store.events import SqliteEventLog
from swarm.types import Event
from swarm.workers import runs as W

_spec = importlib.util.spec_from_file_location(
    "soak", Path(__file__).resolve().parents[1] / "scripts" / "soak.py"
)
soak = importlib.util.module_from_spec(_spec)
sys.modules["soak"] = soak  # dataclasses resolve their module by name
_spec.loader.exec_module(soak)

T0 = datetime(2026, 11, 7, 10, 0, tzinfo=SEOUL)
UNTIL = T0 + timedelta(hours=48)


class Synth:
    """A swarm.db with the events / runs / outbox / projects tables of the real stores."""

    def __init__(self, home: Path) -> None:
        self.home = home
        home.mkdir(parents=True, exist_ok=True)
        self.clock = FakeClock(T0)
        self.db = Database(home / "swarm.db")
        self.log = SqliteEventLog(self.db, BlobStore(home / "blobs", self.clock))
        for name, sql in [
            ("workers_runs_v1", W._SCHEMA), ("workers_runs_v2", W._SCHEMA_V2),
            ("workers_runs_v3", W._SCHEMA_V3), ("front_outbox_v1", N._SCHEMA),
            ("front_outbox_v2", N._SCHEMA_V2), ("projects_v1", P._SCHEMA),
            ("projects_v2", P._SCHEMA_V2),
        ]:  # fmt: skip
            self.db.migrate(name, sql)

    def ev(self, hours: float, kind: str, run_id=None, project_id="prj_a", **payload) -> None:
        self.log.append(
            Event(id="", ts=T0 + timedelta(hours=hours), kind=kind, actor="test",
                  project_id=project_id, run_id=run_id, payload=payload, refs=[])
        )  # fmt: skip

    def run(self, run_id: str, item: str | None, intent: str = "x") -> None:
        order = {"id": f"wo_{run_id}", "backlog_item_id": item, "intent": intent}
        self.db.execute(
            "INSERT INTO runs(id, order_id, project_id, order_json, variant_json, status,"
            " started_at) VALUES (?,?,?,?,?,?,?)",
            (run_id, order["id"], "prj_a", json.dumps(order), "{}", "interrupted",
             T0.isoformat()),
        )  # fmt: skip

    def executed(self, hours: float, run_id: str, fingerprint: str) -> None:
        self.ev(hours, "guard.executed", run_id, None, order_id=f"wo_{run_id}",
                fingerprint=fingerprint)  # fmt: skip

    def done(self, hours: float, run_id: str, evidence: bool = True) -> None:
        if evidence:
            self.ev(hours - 0.1, "run.evidence", run_id, url="http://x")
        self.ev(hours, "run.finished", run_id, status="done")

    def briefing(self, when: datetime, status: str = "sent", ref: str = "all") -> None:
        self.db.execute(
            "INSERT INTO outbox(channel, kind, text, ref_id, urgent, status, created_at, done_at)"
            " VALUES (?,?,?,?,?,?,?,?)",
            ("telegram:-1:1", "report", "일일 보고", f"report:daily:{ref}:{when.date()}", 0,
             status, when.isoformat(), when.isoformat()),
        )  # fmt: skip

    def healthy(self) -> "Synth":
        self.done(5, "r1")
        self.done(30, "r2")
        self.briefing(datetime(2026, 11, 8, 8, 0, 3, tzinfo=SEOUL))
        self.briefing(datetime(2026, 11, 9, 8, 0, 3, tzinfo=SEOUL))
        self.run("r3", "bl_1")
        self.ev(20, "run.interrupted", "r3", **{"from": "running", "to": "interrupted"})
        self.ev(20.1, "agenda.dispatched", None, order_id="wo_x", backlog_item_id="bl_1")
        self.run("r5", None)
        self.ev(21, "run.interrupted", "r5", **{"from": "leased", "to": "interrupted"})
        self.ev(21.2, "run.running", "r5", **{"from": "interrupted", "to": "running"})
        self.run("r1", "bl_r1")
        self.executed(12, "r1", "fp1")
        self.ev(12.5, "guard.already_executed", "r1", order_id="wo_r1", fingerprint="fp1")
        self.executed(13, "r1", "fp2")  # a different action of the same item
        self.ev(25, "watchdog.restart", None, None, at="x")
        self.ev(26, "watchdog.restart", None, None, at="y")
        self.ev(8, "worker.quota_signal", "r4", signal={"kind": "limit_hit"})
        self.ev(8, "engine.quota_exhausted", None, None, engine="claude")
        self.ev(8.01, "run.finished", "r4", status="interrupted")
        self.ev(3.1, "browser.started", None, None, profile="work")
        self.ev(30.2, "browser.reconnected", None, None, profile="work")
        return self

    def state(self) -> dict:
        st = soak.new_plan(T0, 48)
        st["steps"]["chrome_kill_1"].update(done_at=(T0 + timedelta(hours=3)).isoformat(),
                                            note="killed")  # fmt: skip
        st["steps"]["chrome_kill_2"].update(done_at=(T0 + timedelta(hours=30)).isoformat(),
                                            note="killed")  # fmt: skip
        return st

    def check(self, **kw) -> dict[str, soak.Row]:
        kw.setdefault("state", self.state())
        rows = soak.run_checks(self.home / "swarm.db", T0, UNTIL, **kw)
        return {r.name: r for r in rows}


@pytest.fixture
def synth(tmp_path) -> Synth:
    return Synth(tmp_path)


def statuses(rows) -> dict[str, str]:
    return {n: r.status for n, r in rows.items()}


def test_healthy_soak_passes_every_check(synth):
    rows = synth.healthy().check()
    assert set(statuses(rows).values()) == {"PASS"}, statuses(rows)
    assert rows["AC-1 완료 ≥1/일"].detail == "per day 1, 1"
    assert rows["AC-7 interrupted→재개"].detail == "2 interrupted, 2 continued"
    assert rows["중복 외부 행동 0"].detail.startswith("2 executed, 1 repeats stopped")


def test_a_day_without_completion_fails_the_completion_check(synth):
    synth.done(5, "r1")
    row = synth.check()["AC-1 완료 ≥1/일"]
    assert row.status == "FAIL" and row.detail == "per day 1, 0"


def test_less_than_a_day_fails_the_completion_check(synth):
    synth.done(5, "r1")
    rows = soak.run_checks(synth.home / "swarm.db", T0, T0 + timedelta(hours=23))
    assert rows[0].status == "FAIL" and "no full 24 h window" in rows[0].detail


def test_done_without_evidence_fails_the_completion_check(synth):
    synth.healthy().done(40, "r9", evidence=False)
    row = synth.check()["AC-1 완료 ≥1/일"]
    assert row.status == "FAIL" and "r9" in row.detail


def test_project_filter_counts_only_that_project(synth):
    synth.healthy()
    synth.db.execute(
        "INSERT INTO projects(id, slug, name, objective, strategy, autonomy, context, channel,"
        " board, default_profile, default_variant_json, weight, watches_json)"
        " VALUES ('prj_b','other','b','o','',1,'','telegram:-1:9','other','work','{}',2,'[]')"
    )
    assert synth.check(project="other")["AC-1 완료 ≥1/일"].status == "FAIL"
    with pytest.raises(SystemExit):
        synth.check(project="nope")


def test_two_briefings_or_a_missing_one_fail(synth):
    synth.healthy().briefing(datetime(2026, 11, 8, 8, 5, tzinfo=SEOUL), ref="again")
    assert synth.check()["AC-1 브리핑 1/일"].detail == "per day 2, 1"
    s2 = Synth(synth.home.parent / "c")
    s2.briefing(datetime(2026, 11, 8, 8, 0, tzinfo=SEOUL), status="pending")
    assert s2.check()["AC-1 브리핑 1/일"].detail == "per day 0, 0"


def test_interrupted_run_never_continued_fails_the_resume_check_but_late_one_is_pending(synth):
    synth.healthy()
    synth.run("r7", "bl_7")
    synth.ev(30, "run.interrupted", "r7", **{"from": "running", "to": "interrupted"})
    row = synth.check()["AC-7 interrupted→재개"]
    assert row.status == "FAIL" and "r7" in row.detail

    s2 = Synth(synth.home.parent / "d").healthy()
    s2.run("r8", "bl_8")
    s2.ev(47.5, "run.interrupted", "r8", **{"from": "running", "to": "interrupted"})
    row = s2.check()["AC-7 interrupted→재개"]
    assert row.status == "PASS" and "1 pending" in row.detail


def test_no_interruption_at_all_fails_the_resume_check(synth):
    synth.done(5, "r1")
    assert synth.check()["AC-7 interrupted→재개"].status == "FAIL"


def test_duplicate_external_action_fails(synth):
    synth.healthy().executed(40, "r1", "fp1")
    row = synth.check()["중복 외부 행동 0"]
    assert row.status == "FAIL" and "bl_r1/fp1×2" in row.detail


def test_same_action_after_redispatch_under_a_new_order_is_a_duplicate(synth):
    synth.healthy()
    synth.run("r6", "bl_r1")  # the interrupted item re-dispatched: new order wo_r6
    synth.executed(30, "r6", "fp1")
    row = synth.check()["중복 외부 행동 0"]
    assert row.status == "FAIL" and "bl_r1/fp1×2" in row.detail


def test_orders_without_backlog_item_are_keyed_by_intent(synth):
    synth.healthy()
    synth.run("r7", None, intent="폼 제출")
    synth.run("r8", None, intent="다른 일")
    synth.executed(30, "r7", "fp9")
    synth.executed(31, "r8", "fp9")  # same request, different work: not a duplicate
    assert synth.check()["중복 외부 행동 0"].status == "PASS"
    synth.run("r9", None, intent="폼 제출")
    synth.executed(32, "r9", "fp9")
    row = synth.check()["중복 외부 행동 0"]
    assert row.status == "FAIL" and "intent:" in row.detail


def test_no_external_action_is_not_applicable_and_does_not_fail(synth, capsys):
    synth.done(5, "r1")
    row = synth.check()["중복 외부 행동 0"]
    assert row.status == "N/A" and "0 executed" in row.detail
    assert "N/A" in soak.render(list(synth.check().values()))


def test_three_watchdog_restarts_in_one_day_fail(synth):
    synth.healthy().ev(27, "watchdog.restart", None, None, at="z")
    row = synth.check()["watchdog.restart ≤2/일"]
    assert row.status == "FAIL" and "2026-11-08: 3" in row.detail


def test_limit_window_missing_or_ending_in_failure_fails(synth):
    synth.done(5, "r1")
    assert synth.check()["한도 소진 = 쉼(실패 아님)"].status == "FAIL"
    synth.ev(8, "engine.quota_exhausted", None, None, engine="claude")
    synth.ev(8, "worker.quota_signal", "r4", signal={"kind": "limit_hit"})
    synth.ev(9, "run.finished", "r4", status="failed")
    row = synth.check()["한도 소진 = 쉼(실패 아님)"]
    assert row.status == "FAIL" and "r4" in row.detail


def test_chrome_rows_need_soak_json_and_a_recovery_after_each_kill(synth):
    synth.done(5, "r1")
    assert synth.check(state=None)["Chrome 강제 종료 복구"].status == "SKIP"
    synth.ev(3.1, "browser.started", None, None, profile="work")
    row = synth.check()["Chrome 강제 종료 복구"]
    assert row.status == "FAIL" and row.detail == "1/2 kills recovered"


def test_check_command_prints_table_and_exit_code(synth, capsys):
    synth.healthy()
    (synth.home / "soak.json").write_text(json.dumps(synth.state()))
    args = ["check", "--home", str(synth.home), "--until", UNTIL.isoformat()]
    assert soak.main(args) == 0
    out = capsys.readouterr().out
    assert "SOAK PASS" in out and "AC-7" in out
    synth.ev(27, "watchdog.restart", None, None, at="z")
    assert soak.main(args) == 1
    assert "SOAK FAIL (1 check(s))" in capsys.readouterr().out


# --- orchestrate (fake commands and clock) -------------------------------------------------------


class Fake:
    def __init__(self, home: Path) -> None:
        self.now = T0
        self.boot = T0 - timedelta(days=1)
        self.cmds: list[list[str]] = []
        self.rc = {"pkill": 1}
        self.out: list[str] = []
        self.o = soak.Orchestrator(
            home=home, run=self._run, now=lambda: self.now, boot=lambda: self.boot, uid=501,
            log=self.out.append,
        )  # fmt: skip

    def _run(self, argv) -> int:
        self.cmds.append(list(argv))
        return self.rc.get(argv[0], 0)

    def at(self, hours: float) -> None:
        self.now = T0 + timedelta(hours=hours)


def test_orchestrate_runs_every_step_and_resumes_from_soak_json(synth):
    f = Fake(synth.home)
    state = f.o.load_or_create(48)
    assert not f.o.tick(state) and f.cmds == []  # nothing due at the start

    f.at(3)
    f.o.tick(state)  # Chrome not up yet: retried after 5 min, not before
    f.at(3.05)
    f.o.tick(state)
    assert f.cmds == [soak.CHROME_KILL]
    f.rc["pkill"] = 0
    f.at(3.1)
    f.o.tick(state)
    assert state["steps"]["chrome_kill_1"]["note"] == "killed"

    f.cmds.clear()
    f.at(8)
    f.o.tick(state)
    assert f.cmds == [
        ["launchctl", "setenv", "SWARM_SIMULATE_LIMIT", "1"],
        ["launchctl", "kickstart", "-k", "gui/501/com.swarm.swarmd"],
    ]
    f.at(8.02)
    f.o.tick(state)
    assert not state["steps"]["limit_window"]["done_at"]  # swarmd has not booted with it yet
    synth.ev(8.03, "daemon.boot", None, None, pid=1)
    f.at(8.04)
    f.o.tick(state)
    assert f.cmds[-1] == ["launchctl", "unsetenv", "SWARM_SIMULATE_LIMIT"]
    assert state["steps"]["limit_window"]["note"] == ""

    f.at(20)
    f.o.tick(state)
    assert any("shutdown -r now" in line for line in f.out)
    assert not state["steps"]["reboot"]["done_at"]

    # the reboot killed the script: a new one reads soak.json and sees the new boot time
    g = Fake(synth.home)
    g.boot, g.rc["pkill"] = T0 + timedelta(hours=20.5), 0
    state2 = g.o.load_or_create(48)
    g.at(21)
    g.o.tick(state2)
    assert state2["steps"]["reboot"]["done_at"]
    g.at(30)
    assert g.o.tick(state2) is True
    assert g.cmds == [soak.CHROME_KILL]


def test_chrome_kill_gives_up_after_two_hours(synth):
    f = Fake(synth.home)
    state = f.o.load_or_create(48)
    for h in (3, 3.5, 4, 4.5, 5.01):
        f.at(h)
        f.o.tick(state)
    assert state["steps"]["chrome_kill_1"]["note"].startswith("missed")


# --- window edges, kill recovery, dry run -------------------------------------------------------


def test_evidence_posted_before_since_still_counts(synth):
    synth.healthy()
    synth.ev(-0.5, "run.evidence", "r10", url="http://x")  # before the soak window
    synth.ev(10, "run.finished", "r10", status="done")
    assert synth.check()["AC-1 완료 ≥1/일"].status == "PASS"


def test_chrome_recovery_must_follow_the_kill_within_the_bound(synth):
    synth.done(5, "r1")
    synth.ev(3.1, "browser.started", None, None, profile="work")
    synth.ev(30 + 3, "browser.started", None, None, profile="work")  # 3 h after kill 2
    row = synth.check()["Chrome 강제 종료 복구"]
    assert row.status == "FAIL" and row.detail == "1/2 kills recovered"


def test_dry_run_never_writes_soak_json(synth):
    f = Fake(synth.home)
    f.o.persist = False
    state = f.o.load_or_create(48)
    f.rc["pkill"] = 0
    f.at(3)
    f.o.tick(state)
    assert state["steps"]["chrome_kill_1"]["done_at"]  # walked in memory
    assert not (synth.home / "soak.json").exists()


def test_failed_launchctl_ends_the_limit_step_with_a_note(synth):
    f = Fake(synth.home)
    f.rc["launchctl"] = 113
    state = f.o.load_or_create(48)
    f.at(8)
    f.o.tick(state)
    s = state["steps"]["limit_window"]
    assert s["done_at"] and s["note"].startswith("failed: launchctl rc")
    assert f.cmds[-1] == ["launchctl", "unsetenv", "SWARM_SIMULATE_LIMIT"]


def test_soak_day_boundaries_follow_the_configured_timezone(tmp_path):
    assert str(soak.load_tz(tmp_path)) == "Asia/Seoul"  # no config.toml: the default
    (tmp_path / "config.toml").write_text('timezone = "America/New_York"\n')
    tz = soak.load_tz(tmp_path)
    assert str(tz) == "America/New_York"
    ev = soak.Ev(1, datetime(2026, 11, 8, 3, 0, tzinfo=SEOUL), "watchdog.restart", None, None, {})
    assert soak.check_restarts([ev], tz).detail == "2026-11-07: 1"  # 18:00 UTC the day before
