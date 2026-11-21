"""swarm watchdog: a separate launchd job (com.swarm.watchdog, StartInterval=120) that keeps
swarmd honest from the outside. One short pass per start, then it exits:

1. Heartbeat: `<home>/heartbeat` (touched by swarmd after every agenda tick) older than
   5 min -> `launchctl kickstart -k gui/<uid>/com.swarm.swarmd`. A missing file counts from
   the first pass that saw it missing. No second kick within 5 min of the last one (the
   restarted daemon needs a moment to tick). From the 2nd restart of a day
   one Telegram FYI per day goes to General — sent straight to the Bot API
   (plain text) because the daemon is the thing that is broken.
2. Stuck runs (read-only on swarm.db): a `running` Run whose last `worker.tool_result` (or
   its last entry into `running`) is older than 15 min, or whose last 6 tool calls in this
   segment are the same call (same tool, same arguments) -> `POST /internal/runs/{id}/cancel`
   with reason "stuck" (bearer = <home>/secret). The same run is not re-cancelled for 10 min.

State (restart counts, the last kick, cancel attempts, recent restart times) lives in
`<home>/watchdog.json`; swarmd copies each new restart into its event log as
`watchdog.restart` at boot (the watchdog itself never writes swarm.db).
The bot token is never logged.
"""

import argparse
import json
import logging
import os
import sqlite3
import subprocess
import sys
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx

from swarm.clock import AlertWindow, Clock, SystemClock
from swarm.config import Config, load_config
from swarm.front.notifier import parse_channel
from swarm.redact import add_secret
from swarm.workers.contract import STUCK_REASON

log = logging.getLogger("swarm.watchdog")

LABEL = "com.swarm.swarmd"
STATE_NAME = "watchdog.json"
STALE_AFTER = timedelta(minutes=5)
RESTART_GRACE = timedelta(minutes=5)
STUCK_AFTER = timedelta(minutes=15)
SAME_CALL_LIMIT = 6
RECANCEL_AFTER = timedelta(minutes=10)
HISTORY_MAX = 50
ALERT_AFTER_RESTARTS = 2  # the second restart in a day alerts
TELEGRAM_API = "https://api.telegram.org"
TEXT_MAX = 3_500
CANCEL_LOG_MAX = 200  # a cancel error body, in the log

_TS = "%Y-%m-%dT%H:%M:%S.%f"  # events.ts: UTC, fixed width (store/events.py)


def _ts_key(at: datetime) -> str:
    return at.astimezone(UTC).strftime(_TS)


def _parse_ts(raw: str) -> datetime:
    return datetime.strptime(raw, _TS).replace(tzinfo=UTC)


@dataclass
class WatchdogState:
    day: str = ""
    restarts: int = 0
    alerted_day: str | None = None
    last_restart_at: str | None = None
    missing_since: str | None = None
    cancelled: dict[str, str] = field(default_factory=dict)  # run id -> last attempt
    history: list[str] = field(default_factory=list)  # restart times; swarmd logs each once

    @classmethod
    def load(cls, path: Path) -> "WatchdogState":
        try:
            raw = json.loads(path.read_text())
            return cls(**{k: v for k, v in raw.items() if k in cls.__dataclass_fields__})
        except (FileNotFoundError, ValueError, TypeError):
            return cls()

    def save(self, path: Path) -> None:
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(asdict(self), ensure_ascii=False, indent=1))
        os.replace(tmp, path)


# --- side effects (injectable) ----------------------------------------------------------------


def launchctl_kickstart(label: str = LABEL) -> bool:
    target = f"gui/{os.getuid()}/{label}"
    try:
        p = subprocess.run(
            ["launchctl", "kickstart", "-k", target], capture_output=True, text=True, timeout=30
        )
    except (OSError, subprocess.TimeoutExpired) as e:
        log.error("launchctl kickstart %s failed: %s", target, type(e).__name__)
        return False
    if p.returncode != 0:
        log.error("launchctl kickstart %s exited %s: %s", target, p.returncode, p.stderr[-300:])
    return p.returncode == 0


def send_telegram(
    token: str, channel: str, text: str, *, silent: bool = False,
    transport: httpx.BaseTransport | None = None,
) -> bool:  # fmt: skip
    """One plain-text message (no parse_mode) to `channel`; `silent` = no sound (outside the
    alert window). Never logs the token (swarm.redact scrubs the request loggers)."""
    try:
        chat, thread = parse_channel(channel)
    except ValueError as e:
        log.error("watchdog alert: %s", e)
        return False
    add_secret(token)
    body: dict = {"chat_id": chat, "text": text[:TEXT_MAX]}
    if silent:
        body["disable_notification"] = True
    if thread is not None:
        body["message_thread_id"] = thread
    try:
        with httpx.Client(base_url=TELEGRAM_API, timeout=10, transport=transport) as c:
            r = c.post(f"/bot{token}/sendMessage", json=body)
        ok = r.status_code == 200 and bool(r.json().get("ok"))
    except (httpx.HTTPError, ValueError) as e:
        log.error("watchdog alert not sent: %s", type(e).__name__)  # str(e) has the URL
        return False
    if not ok:
        log.error("watchdog alert not sent: HTTP %s", r.status_code)
    return ok


def cancel_via_internal(
    port: int, secret: str, run_id: str, reason: str, *,
    transport: httpx.BaseTransport | None = None,
) -> bool:  # fmt: skip
    url = f"http://127.0.0.1:{port}/internal/runs/{run_id}/cancel"
    try:
        with httpx.Client(timeout=15, transport=transport) as c:
            r = c.post(url, json={"reason": reason}, headers={"Authorization": f"Bearer {secret}"})
    except httpx.HTTPError as e:
        log.error("cancel %s failed: %s", run_id, type(e).__name__)
        return False
    if r.status_code not in (200, 202):
        log.error("cancel %s: HTTP %s %s", run_id, r.status_code, r.text[:CANCEL_LOG_MAX])
    return r.status_code in (200, 202)


# --- the checks -------------------------------------------------------------------------------


class Watchdog:
    """One pass of heartbeat and stuck-run checks. `clock` is in the configured timezone: its
    calendar day is the day the restart count resets on."""

    def __init__(
        self,
        *,
        home: Path,
        clock: Clock,
        kickstart: Callable[[], bool],
        alert: Callable[[str], bool],
        cancel: Callable[[str, str], bool],
        db_path: Path | None = None,
        state_path: Path | None = None,
    ) -> None:
        self.home = home
        self.clock = clock
        self._kickstart = kickstart
        self._alert = alert
        self._cancel = cancel
        self.db_path = db_path or home / "swarm.db"
        self.state_path = state_path or home / STATE_NAME
        self.heartbeat = home / "heartbeat"

    # 1. heartbeat ---------------------------------------------------------------------------

    def _stale(self, st: WatchdogState, now: datetime) -> bool:
        try:
            mtime = self.heartbeat.stat().st_mtime
        except FileNotFoundError:  # counts from the first pass that saw it missing
            st.missing_since = st.missing_since or now.isoformat()
            return now - datetime.fromisoformat(st.missing_since) > STALE_AFTER
        st.missing_since = None
        return now.timestamp() - mtime > STALE_AFTER.total_seconds()

    def check_heartbeat(self, st: WatchdogState) -> bool:
        """True when swarmd was restarted."""
        now = self.clock.now()
        if not self._stale(st, now):
            return False
        if st.last_restart_at and now - datetime.fromisoformat(st.last_restart_at) < RESTART_GRACE:
            return False  # just kicked: give it time to tick
        log.warning("swarmd heartbeat stale: restarting %s", LABEL)
        if not self._kickstart():
            return False
        today = now.date().isoformat()
        if st.day != today:
            st.day, st.restarts = today, 0
        st.restarts += 1
        st.last_restart_at = now.isoformat()
        st.history = [*st.history, st.last_restart_at][-HISTORY_MAX:]
        st.missing_since = None
        if st.restarts >= ALERT_AFTER_RESTARTS and st.alerted_day != today:
            text = (
                f"⚠️ FYI · swarmd가 오늘 {st.restarts}번 멈춰서 watchdog이 다시 띄웠어요.\n"
                "맥미니 ~/swarm/logs/swarmd.err.log 를 확인해 주세요."
            )
            if self._alert(text):
                st.alerted_day = today
        return True

    # 2. stuck runs --------------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection | None:
        if not self.db_path.exists():
            return None
        c = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True, timeout=5)
        c.row_factory = sqlite3.Row
        return c

    def stuck_runs(self) -> list[tuple[str, str]]:
        """(run_id, why) for running Runs that look stuck."""
        now = self.clock.now()
        c = self._connect()
        if c is None:
            return []
        out: list[tuple[str, str]] = []
        try:
            try:
                runs = c.execute("SELECT id FROM runs WHERE status='running'").fetchall()
            except sqlite3.OperationalError:  # no runs table yet
                return []
            for r in runs:
                rid = r["id"]
                seg = c.execute(
                    "SELECT MAX(seq) FROM events WHERE run_id=? AND kind='run.running'", (rid,)
                ).fetchone()[0]
                if seg is None:
                    continue  # never really started a turn
                last = c.execute(
                    "SELECT MAX(ts) FROM events WHERE run_id=? AND seq>=? "
                    "AND kind IN ('worker.tool_result','run.running')",
                    (rid, seg),
                ).fetchone()[0]
                if last is not None and now - _parse_ts(last) > STUCK_AFTER:
                    out.append((rid, f"tool_result 없음 {STUCK_AFTER.seconds // 60}분"))
                    continue
                calls = c.execute(
                    "SELECT payload_json FROM events WHERE run_id=? AND seq>? "
                    "AND kind='worker.tool_call' ORDER BY seq DESC LIMIT ?",
                    (rid, seg, SAME_CALL_LIMIT),
                ).fetchall()
                if len(calls) == SAME_CALL_LIMIT:
                    keys = set()
                    for row in calls:
                        p = json.loads(row["payload_json"])
                        args = json.dumps(p.get("args"), sort_keys=True, ensure_ascii=False)
                        keys.add((p.get("name"), args))
                    if len(keys) == 1:
                        name = next(iter(keys))[0]
                        out.append((rid, f"같은 도구 호출 {SAME_CALL_LIMIT}회 연속: {name}"))
        finally:
            c.close()
        return out

    def check_stuck(self, st: WatchdogState) -> list[str]:
        now = self.clock.now()
        done: list[str] = []
        stuck = self.stuck_runs()
        live = {rid for rid, _ in stuck}
        st.cancelled = {k: v for k, v in st.cancelled.items() if k in live}
        for rid, why in stuck:
            prev = st.cancelled.get(rid)
            if prev and now - datetime.fromisoformat(prev) < RECANCEL_AFTER:
                continue
            log.warning("run %s looks stuck (%s): cancelling", rid, why)
            st.cancelled[rid] = now.isoformat()
            if self._cancel(rid, STUCK_REASON):
                done.append(rid)
        return done

    def run_once(self) -> dict:
        st = WatchdogState.load(self.state_path)
        restarted = self.check_heartbeat(st)
        cancelled = [] if restarted else self.check_stuck(st)  # the daemon is coming back up
        st.save(self.state_path)
        return {"restarted": restarted, "cancelled": cancelled, "restarts_today": st.restarts}


def alert_for(cfg: Config, *, clock: Clock | None = None) -> Callable[[str], bool]:
    """The restart FYI: urgent in content, but silent outside the alert window (08-23)."""
    tz = ZoneInfo(cfg.timezone)
    window, clock = AlertWindow(cfg.alert_start, cfg.alert_end, tz), clock or SystemClock(tz)

    def alert(text: str) -> bool:
        silent = not window.is_open(clock.now())
        return send_telegram(cfg.telegram_bot_token, cfg.general_channel, text, silent=silent)

    return alert


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="swarm-watchdog", description="swarmd watchdog (one pass)")
    p.add_argument("--config", type=Path, default=None)
    a = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    path = a.config or Path(os.environ.get("SWARM_CONFIG", "~/swarm/config.toml")).expanduser()
    try:
        cfg = load_config(path)
        tz = ZoneInfo(cfg.timezone)
        secret = (cfg.home / "secret").read_text().strip()
    except Exception as e:  # noqa: BLE001
        log.error("watchdog: cannot read config/secret: %s", type(e).__name__)
        return 2
    wd = Watchdog(
        home=cfg.home,
        clock=SystemClock(tz),
        kickstart=launchctl_kickstart,
        alert=alert_for(cfg),
        cancel=lambda rid, why: cancel_via_internal(cfg.internal_port, secret, rid, why),
    )
    result = wd.run_once()
    log.info("watchdog pass: %s", result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
