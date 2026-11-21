"""48-hour soak for swarmd. Run it on the machine that hosts swarmd.

    uv run python scripts/soak.py orchestrate [--home ~/swarm] [--hours 48] [--dry-run]
    uv run python scripts/soak.py check [--home ~/swarm] [--since ISO] [--until ISO]
                                         [--project SLUG] [--grace-min 60]

orchestrate: writes the plan to <home>/soak.json, then works through it, one step at a time.
  The steps are two Chrome kills (`pkill -f "user-data-dir=.*work"`), one usage-limit window,
  and one reboot. For the limit window it runs `launchctl setenv SWARM_SIMULATE_LIMIT 1` plus
  `kickstart -k`, then `unsetenv` once swarmd has booted with it. The first engine call then
  reports a limit and the ledger cools the engine for an hour. The reboot is a person's job
  (`sudo shutdown -r now`): the script prints when to do it, then sees from `kern.boottime`
  that it happened. A reboot kills this script, so run it again afterwards. It reads soak.json
  and carries on where it stopped.

check: reads <home>/swarm.db read-only for [since, until). It prints a pass/fail table and
  exits 1 when any check fails.
  - AC-1: every full 24 h window has >= 1 WorkOrder done (`run.finished` status=done), and
    every done run has evidence (`run.evidence`). It also has exactly one 08:00 briefing sent.
    No event marks a sent briefing, so the check reads the durable outbox row
    (`ref_id` report:daily:*, status sent).
  - AC-7: every `run.interrupted` continues. Either the same run goes on (`run.resumed`, or a
    later status change), or its backlog item is dispatched again (`agenda.dispatched`). An
    interruption less than --grace-min before `until` counts as pending, not failed. Zero
    interruptions fail, because AC-7 was then never exercised.
  - external actions: no `guard.executed` twice for the same work + fingerprint. Work = the
    order's backlog item, else a hash of its intent, so a repeat after a re-dispatch under a
    new order_id counts. No external action at all prints N/A (visible, does not fail).
  - `watchdog.restart` <= 2 per calendar day (the config timezone).
  - limit window: >= 1 `engine.quota_exhausted`, and no run that got a `worker.quota_signal`
    ended `failed` (a limit is a pause, not a failure).
  - Chrome kills (needs soak.json): each kill is followed within 2 h by `browser.started` or
    `browser.reconnected`.
"""

import argparse
import hashlib
import json
import os
import re
import sqlite3
import subprocess
import sys
import time
import tomllib
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

DEFAULT_TIMEZONE = "Asia/Seoul"  # the default of `timezone` in <home>/config.toml
SEOUL = ZoneInfo(DEFAULT_TIMEZONE)
STATE_NAME = "soak.json"
LABEL = "com.swarm.swarmd"
LIMIT_ENV = "SWARM_SIMULATE_LIMIT"
CHROME_KILL = ["pkill", "-f", "user-data-dir=.*work"]
CHROME_RETRY = timedelta(minutes=5)  # Chrome may not be up between runs: try again for 2 h
CHROME_GIVE_UP = timedelta(hours=2)
LIMIT_BOOT_WAIT = timedelta(minutes=10)
REBOOT_REMIND = timedelta(hours=1)
# Chrome comes back at the next session open (reconnect or a fresh launch); a pilot that is
# working opens one well within this. A start much later is unrelated to the kill.
CHROME_RECOVERY = timedelta(hours=2)
POLL_S = 30.0
DAY = timedelta(hours=24)

# step name -> hours after the start (the reboot sits between the two Chrome kills)
PLAN = {"chrome_kill_1": 3, "limit_window": 8, "reboot": 20, "chrome_kill_2": 30}

Run = Callable[[Sequence[str]], int]


def load_tz(home: Path) -> ZoneInfo:
    """The `timezone` of <home>/config.toml (swarmd's own setting), else the default."""
    try:
        name = tomllib.loads((home / "config.toml").read_text())["timezone"]
    except (OSError, KeyError, tomllib.TOMLDecodeError):
        name = DEFAULT_TIMEZONE
    return ZoneInfo(name)


def _now(tz: ZoneInfo = SEOUL) -> datetime:
    return datetime.now(tz)


def _parse(ts: str, tz: ZoneInfo = SEOUL) -> datetime:
    d = datetime.fromisoformat(ts)
    return d if d.tzinfo else d.replace(tzinfo=tz)


def _db_ts(dt: datetime) -> str:
    """The event log's ts column: UTC, fixed width (swarm.store.events._ts_key)."""
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")


def _from_db_ts(s: str, tz: ZoneInfo = SEOUL) -> datetime:
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=UTC).astimezone(tz)


# --- orchestrate -------------------------------------------------------------------------------


def new_plan(start: datetime, hours: int) -> dict:
    return {
        "started_at": start.isoformat(),
        "hours": hours,
        "steps": {
            name: {"at": (start + timedelta(hours=h)).isoformat(), "done_at": None, "note": ""}
            for name, h in PLAN.items()
        },
    }


def boot_time(tz: ZoneInfo = SEOUL) -> datetime:
    out = subprocess.run(["sysctl", "-n", "kern.boottime"], capture_output=True, text=True)
    m = re.search(r"sec = (\d+)", out.stdout)
    return datetime.fromtimestamp(int(m.group(1)), tz) if m else datetime.min.replace(tzinfo=UTC)


def _shell(argv: Sequence[str]) -> int:
    print("  $ " + " ".join(argv), flush=True)
    return subprocess.run(list(argv)).returncode


def _dry(argv: Sequence[str]) -> int:
    print("  (dry-run) $ " + " ".join(argv), flush=True)
    return 0


def _booted_since(db: Path, since: datetime) -> bool:
    try:
        c = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        try:
            return (
                c.execute(
                    "SELECT 1 FROM events WHERE kind='daemon.boot' AND ts>? LIMIT 1",
                    (_db_ts(since),),
                ).fetchone()
                is not None
            )
        finally:
            c.close()
    except sqlite3.Error:
        return False


REBOOT_TEXT = """
==================================================================================
 🔁 사람 차례: 맥미니를 재부팅해 주세요 (AC-7)
    1) `uv run swarm status` 로 running Run이 하나 이상인지 확인 (없으면 몇 분 뒤 다시)
    2) sudo shutdown -r now
    3) 로그인 후:  cd ~/browser-swarm && uv run python scripts/soak.py orchestrate
       (soak.json을 읽고 이어서 진행합니다)
==================================================================================
"""


@dataclass
class Orchestrator:
    home: Path
    tz: ZoneInfo = SEOUL
    run: Run = _shell
    now: Callable[[], datetime] = _now
    boot: Callable[[], datetime] = boot_time
    uid: int = field(default_factory=os.getuid)
    log: Callable[[str], None] = print
    persist: bool = True  # False (--dry-run): walk the plan in memory, never touch soak.json

    @property
    def state_path(self) -> Path:
        return self.home / STATE_NAME

    def load_or_create(self, hours: int) -> dict:
        if self.state_path.exists():
            return json.loads(self.state_path.read_text())
        state = new_plan(self.now(), hours)
        self.save(state)
        return state

    def save(self, state: dict) -> None:
        if self.persist:
            self.state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2))

    def describe(self, state: dict) -> str:
        lines = [f"soak started {state['started_at']} for {state['hours']} h"]
        for name, s in state["steps"].items():
            done = f"done {s['done_at']} {s['note']}".rstrip() if s["done_at"] else "pending"
            lines.append(f"  {name:<14} at {s['at']}  {done}")
        end = _parse(state["started_at"], self.tz) + timedelta(hours=state["hours"])
        lines.append(f"  check after   {end.isoformat()}: uv run python scripts/soak.py check")
        return "\n".join(lines)

    def _done(self, state: dict, name: str, note: str = "") -> None:
        state["steps"][name].update(done_at=self.now().isoformat(), note=note)
        self.save(state)
        self.log(f"[{self.now():%m-%d %H:%M}] {name}: done {note}".rstrip())

    def tick(self, state: dict) -> bool:
        """Advance every due step once. True when all steps are done."""
        now = self.now()
        for name, s in state["steps"].items():
            if s["done_at"] or now < _parse(s["at"], self.tz):
                continue
            if name.startswith("chrome_kill"):
                self._chrome_kill(state, name, s, now)
            elif name == "limit_window":
                self._limit_window(state, s, now)
            elif name == "reboot":
                self._reboot(state, s, now)
        return all(s["done_at"] for s in state["steps"].values())

    def _chrome_kill(self, state: dict, name: str, s: dict, now: datetime) -> None:
        last = s.get("tried_at")
        if last and now - _parse(last, self.tz) < CHROME_RETRY:
            return
        s["tried_at"] = now.isoformat()
        rc = self.run(CHROME_KILL)
        if rc == 0:
            self._done(state, name, "killed")
        elif now - _parse(s["at"], self.tz) >= CHROME_GIVE_UP:
            self._done(state, name, "missed: Chrome never running")
        else:
            self.save(state)
            self.log(f"  {name}: no Chrome with a 'work' profile yet, retry in 5 min")

    def _limit_window(self, state: dict, s: dict, now: datetime) -> None:
        target = f"gui/{self.uid}/{LABEL}"
        if not s.get("set_at"):
            rcs = [self.run(["launchctl", "setenv", LIMIT_ENV, "1"]),
                   self.run(["launchctl", "kickstart", "-k", target])]  # fmt: skip
            if any(rcs):
                self.run(["launchctl", "unsetenv", LIMIT_ENV])
                self._done(state, "limit_window", f"failed: launchctl rc {rcs}")
                return
            s["set_at"] = now.isoformat()
            self.save(state)
            return
        set_at = _parse(s["set_at"], self.tz)
        booted = _booted_since(self.home / "swarm.db", set_at)
        if booted or now - set_at >= LIMIT_BOOT_WAIT:
            self.run(["launchctl", "unsetenv", LIMIT_ENV])  # later restarts run unlimited
            self._done(state, "limit_window", "" if booted else "warning: no daemon.boot seen")

    def _reboot(self, state: dict, s: dict, now: datetime) -> None:
        prompted = s.get("prompted_at")
        if prompted and self.boot() > _parse(prompted, self.tz):
            self._done(state, "reboot", f"boot at {self.boot().isoformat()}")
            return
        if not prompted or now - _parse(s.get("reminded_at") or prompted, self.tz) >= REBOOT_REMIND:
            self.log(REBOOT_TEXT)
            s["prompted_at"] = prompted or now.isoformat()
            s["reminded_at"] = now.isoformat()
            self.save(state)


def cmd_orchestrate(a: argparse.Namespace) -> int:
    home = Path(a.home).expanduser()
    if a.reset and (home / STATE_NAME).exists():
        (home / STATE_NAME).unlink()
    tz = load_tz(home)
    o = Orchestrator(
        home=home, tz=tz, run=_dry if a.dry_run else _shell, persist=not a.dry_run,
        now=lambda: _now(tz), boot=lambda: boot_time(tz),
    )  # fmt: skip
    state = o.load_or_create(a.hours)
    print(o.describe(state))
    print(REBOOT_TEXT.replace("🔁 사람 차례", f"🔁 예정 {state['steps']['reboot']['at']}"))
    while not o.tick(state):
        if a.once:
            return 0
        time.sleep(POLL_S)
    print(o.describe(state))
    return 0


# --- check -------------------------------------------------------------------------------------


@dataclass
class Row:
    name: str
    status: str  # PASS | FAIL | SKIP | N/A (only FAIL fails the run)
    detail: str


@dataclass
class Ev:
    seq: int
    ts: datetime
    kind: str
    project_id: str | None
    run_id: str | None
    payload: dict


def _table_exists(c: sqlite3.Connection, name: str) -> bool:
    q = "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?"
    return c.execute(q, (name,)).fetchone() is not None


def load_events(
    c: sqlite3.Connection, since: datetime, until: datetime, tz: ZoneInfo = SEOUL
) -> list[Ev]:
    rows = c.execute(
        "SELECT seq, ts, kind, project_id, run_id, payload_json FROM events "
        "WHERE ts>=? AND ts<? ORDER BY seq",
        (_db_ts(since), _db_ts(until)),
    ).fetchall()
    return [Ev(r[0], _from_db_ts(r[1], tz), r[2], r[3], r[4], json.loads(r[5])) for r in rows]


def evidence_runs(c: sqlite3.Connection, until: datetime) -> Counter:
    rows = c.execute(
        "SELECT run_id FROM events WHERE kind='run.evidence' AND ts<?", (_db_ts(until),)
    )
    return Counter(r[0] for r in rows)


def windows(since: datetime, until: datetime) -> list[tuple[datetime, datetime]]:
    out, start = [], since
    while start + DAY <= until:
        out.append((start, start + DAY))
        start += DAY
    return out


def check_ac1_done(evs: list[Ev], since, until, project_id: str | None, evidence: Counter) -> Row:
    """`evidence`: run.evidence per run up to `until`, with no lower bound (a run may post it
    before `since` and finish inside the window)."""
    wins = windows(since, until)
    if not wins:
        return Row("AC-1 완료 ≥1/일", "FAIL", "no full 24 h window between since and until")
    done = [
        e
        for e in evs
        if e.kind == "run.finished"
        and e.payload.get("status") == "done"
        and (project_id is None or e.project_id == project_id)
    ]
    counts = [sum(1 for e in done if a <= e.ts < b) for a, b in wins]
    no_ev = sorted({e.run_id for e in done if not evidence[e.run_id]})
    ok = all(n >= 1 for n in counts) and not no_ev
    detail = "per day " + ", ".join(str(n) for n in counts)
    if no_ev:
        detail += f"; done without evidence: {', '.join(no_ev[:5])}"
    return Row("AC-1 완료 ≥1/일", "PASS" if ok else "FAIL", detail)


def check_ac1_briefing(c: sqlite3.Connection, since, until, tz: ZoneInfo = SEOUL) -> Row:
    wins = windows(since, until)
    if not wins:
        return Row("AC-1 브리핑 1/일", "FAIL", "no full 24 h window between since and until")
    sent: list[datetime] = []
    if _table_exists(c, "outbox"):
        for (done_at,) in c.execute(
            "SELECT done_at FROM outbox WHERE ref_id LIKE 'report:daily:%' AND status='sent'"
        ):
            if done_at:
                sent.append(_parse(done_at, tz))
    counts = [sum(1 for t in sent if a <= t < b) for a, b in wins]
    ok = all(n == 1 for n in counts)
    return Row(
        "AC-1 브리핑 1/일", "PASS" if ok else "FAIL", "per day " + ", ".join(map(str, counts))
    )


def _backlog_of_runs(c: sqlite3.Connection) -> dict[str, str | None]:
    if not _table_exists(c, "runs"):
        return {}
    out: dict[str, str | None] = {}
    for run_id, order_json in c.execute("SELECT id, order_json FROM runs"):
        try:
            out[run_id] = json.loads(order_json).get("backlog_item_id")
        except (ValueError, AttributeError):
            out[run_id] = None
    return out


def check_ac7(c: sqlite3.Connection, evs: list[Ev], until: datetime, grace: timedelta) -> Row:
    items = _backlog_of_runs(c)
    stops = [e for e in evs if e.kind == "run.interrupted"]
    if not stops:
        return Row(
            "AC-7 interrupted→재개",
            "FAIL",
            "no run.interrupted observed (reboot with a running Run)",
        )
    lost, pending, cont = [], [], 0
    for s in stops:
        item = items.get(s.run_id or "")
        later = [e for e in evs if e.seq > s.seq]
        same_run = any(
            e.run_id == s.run_id and (e.kind == "run.resumed" or e.kind.startswith("run.") and
                                      e.payload.get("from") == "interrupted")
            for e in later
        )  # fmt: skip
        redispatched = item is not None and any(
            e.kind == "agenda.dispatched" and e.payload.get("backlog_item_id") == item
            for e in later
        )
        if same_run or redispatched:
            cont += 1
        elif until - s.ts < grace:
            pending.append(s.run_id)
        else:
            lost.append(s.run_id)
    detail = f"{len(stops)} interrupted, {cont} continued"
    if pending:
        detail += f", {len(pending)} pending (< grace)"
    if lost:
        detail += f"; never continued: {', '.join(str(r) for r in lost[:5])}"
    return Row("AC-7 interrupted→재개", "FAIL" if lost else "PASS", detail)


def _work_of_orders(c: sqlite3.Connection) -> dict[str, str]:
    """order_id -> the work it does: its backlog item, else a hash of its intent. A re-dispatch
    after a reboot gets a new order_id for the same item, so a repeat shows up as the same work."""
    if not _table_exists(c, "runs"):
        return {}
    out: dict[str, str] = {}
    for order_id, order_json in c.execute("SELECT order_id, order_json FROM runs"):
        try:
            order = json.loads(order_json)
        except ValueError:
            continue
        item = order.get("backlog_item_id") if isinstance(order, dict) else None
        intent = str(order.get("intent", "")) if isinstance(order, dict) else ""
        out[order_id] = item or "intent:" + hashlib.sha1(intent.encode()).hexdigest()[:10]
    return out


def check_duplicates(c: sqlite3.Connection, evs: list[Ev]) -> Row:
    work = _work_of_orders(c)
    executed = Counter(
        (
            work.get(e.payload.get("order_id"), e.payload.get("order_id")),
            e.payload.get("fingerprint"),
        )
        for e in evs
        if e.kind == "guard.executed"
    )
    dups = {k: n for k, n in executed.items() if n > 1}
    stopped = sum(1 for e in evs if e.kind == "guard.already_executed")
    total = sum(executed.values())
    detail = f"{total} executed, {stopped} repeats stopped by the guard"
    if not total:
        return Row("중복 외부 행동 0", "N/A", detail + " (no external action this soak)")
    if dups:
        detail += "; duplicated: " + ", ".join(
            f"{w}/{str(f)[:12]}×{n}" for (w, f), n in dups.items()
        )
    return Row("중복 외부 행동 0", "FAIL" if dups else "PASS", detail)


def check_restarts(evs: list[Ev], tz: ZoneInfo = SEOUL) -> Row:
    per_day = Counter(
        e.ts.astimezone(tz).date().isoformat() for e in evs if e.kind == "watchdog.restart"
    )
    over = {d: n for d, n in per_day.items() if n > 2}
    detail = ", ".join(f"{d}: {n}" for d, n in sorted(per_day.items())) or "none"
    return Row("watchdog.restart ≤2/일", "FAIL" if over else "PASS", detail)


def check_limit(evs: list[Ev]) -> Row:
    exhausted = sum(1 for e in evs if e.kind == "engine.quota_exhausted")
    signalled = {e.run_id: e.seq for e in evs if e.kind == "worker.quota_signal" and e.run_id}
    failed = sorted(
        r
        for r, seq in signalled.items()
        if any(
            e.run_id == r and e.seq > seq and e.kind == "run.finished"
            and e.payload.get("status") == "failed"
            for e in evs
        )
    )  # fmt: skip
    detail = f"{exhausted} engine.quota_exhausted, {len(signalled)} run(s) paused by a limit"
    if failed:
        detail += f"; failed after a limit: {', '.join(failed[:5])}"
    ok = exhausted >= 1 and not failed
    if not exhausted:
        detail += " (limit window never seen: was SWARM_SIMULATE_LIMIT set at boot?)"
    return Row("한도 소진 = 쉼(실패 아님)", "PASS" if ok else "FAIL", detail)


def check_chrome(evs: list[Ev], state: dict | None, tz: ZoneInfo = SEOUL) -> Row:
    if not state:
        return Row("Chrome 강제 종료 복구", "SKIP", f"no {STATE_NAME}: kill times unknown")
    kills = [
        _parse(s["done_at"], tz)
        for n, s in state["steps"].items()
        if n.startswith("chrome_kill") and s.get("done_at") and s.get("note") == "killed"
    ]
    if not kills:
        return Row("Chrome 강제 종료 복구", "FAIL", "no Chrome kill executed (see soak.json notes)")
    recovered = [
        any(
            e.kind in ("browser.started", "browser.reconnected") and k < e.ts <= k + CHROME_RECOVERY
            for e in evs
        )
        for k in kills
    ]
    ok = all(recovered)
    return Row(
        "Chrome 강제 종료 복구",
        "PASS" if ok else "FAIL",
        f"{sum(recovered)}/{len(kills)} kills recovered",
    )


def run_checks(
    db: Path,
    since: datetime,
    until: datetime,
    *,
    project: str | None = None,
    grace: timedelta = timedelta(minutes=60),
    state: dict | None = None,
    tz: ZoneInfo = SEOUL,
) -> list[Row]:
    c = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        pid = None
        if project:
            r = c.execute("SELECT id FROM projects WHERE slug=?", (project,)).fetchone()
            if r is None:
                raise SystemExit(f"soak check: no project with slug {project!r}")
            pid = r[0]
        evs = load_events(c, since, until, tz)
        return [
            check_ac1_done(evs, since, until, pid, evidence_runs(c, until)),
            check_ac1_briefing(c, since, until, tz),
            check_ac7(c, evs, until, grace),
            check_duplicates(c, evs),
            check_restarts(evs, tz),
            check_limit(evs),
            check_chrome(evs, state, tz),
        ]
    finally:
        c.close()


def render(rows: list[Row]) -> str:
    w = max(len(r.name) for r in rows)
    lines = [f"{'check':<{w}}  result  detail", "-" * (w + 40)]
    lines += [f"{r.name:<{w}}  {r.status:<6}  {r.detail}" for r in rows]
    failed = sum(r.status == "FAIL" for r in rows)
    lines.append("-" * (w + 40))
    lines.append("SOAK PASS" if not failed else f"SOAK FAIL ({failed} check(s))")
    return "\n".join(lines)


def cmd_check(a: argparse.Namespace) -> int:
    home = Path(a.home).expanduser()
    state_path = home / STATE_NAME
    state = json.loads(state_path.read_text()) if state_path.exists() else None
    tz = load_tz(home)
    if a.since:
        since = _parse(a.since, tz)
    elif state:
        since = _parse(state["started_at"], tz)
    else:
        print("soak check: --since is required without soak.json", file=sys.stderr)
        return 2
    until = _parse(a.until, tz) if a.until else _now(tz)
    if not (home / "swarm.db").exists():
        print(f"soak check: no {home / 'swarm.db'} (has swarmd run here?)", file=sys.stderr)
        return 2
    rows = run_checks(
        home / "swarm.db", since, until, project=a.project,
        grace=timedelta(minutes=a.grace_min), state=state, tz=tz,
    )  # fmt: skip
    print(f"swarm.db {home / 'swarm.db'}  window {since.isoformat()} .. {until.isoformat()}")
    print(render(rows))
    return 1 if any(r.status == "FAIL" for r in rows) else 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="soak.py", description="swarmd 48 h soak")
    sub = p.add_subparsers(dest="cmd", required=True)
    o = sub.add_parser("orchestrate", help="inject faults on schedule (resumable)")
    o.add_argument("--home", default="~/swarm")
    o.add_argument("--hours", type=int, default=48)
    o.add_argument(
        "--dry-run", action="store_true", help="print the commands; run and save nothing"
    )
    o.add_argument("--once", action="store_true", help="one pass over due steps, then exit")
    o.add_argument("--reset", action="store_true", help="drop soak.json and plan anew")
    o.set_defaults(func=cmd_orchestrate)
    c = sub.add_parser("check", help="evaluate the event log after the run")
    c.add_argument("--home", default="~/swarm")
    c.add_argument("--since", help="ISO time (default: soak.json started_at)")
    c.add_argument("--until", help="ISO time (default: now)")
    c.add_argument("--project", help="count completions of this project slug only")
    c.add_argument("--grace-min", type=int, default=60)
    c.set_defaults(func=cmd_check)
    return p


def main(argv: list[str] | None = None) -> int:
    a = build_parser().parse_args(argv)
    return a.func(a)


if __name__ == "__main__":
    sys.exit(main())
