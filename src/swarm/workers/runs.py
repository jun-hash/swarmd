"""RunTable: the `runs` table, its status machine and its row codecs.

`transition` is the only writer of `runs.status` and checks `ALLOWED_TRANSITIONS`; `update`
writes the other columns of an unfinished run only. Each reads, checks and writes in one
transaction. Finished runs (done/failed/cancelled) never change again.
"""

import contextlib
import json
from datetime import datetime
from typing import Any

from swarm.clock import Clock
from swarm.store.codec import to_json
from swarm.store.db import Database
from swarm.store.events import Emitter
from swarm.types import Evidence, ProjectId, RunId, RunResult, Variant, WorkerRun, WorkOrder
from swarm.workers.contract import FINISHED, IllegalTransition, RunFinished, order_from_json

STATUSES = frozenset(
    {
        "leased",
        "running",
        "verifying",
        "waiting_human",
        "done",
        "interrupted",
        "failed",
        "cancelled",
    }
)
ACTIVE = frozenset({"leased", "running", "verifying", "waiting_human"})
# The run state diagram. `None` = the row just created; cancelled is reachable from any
# unfinished state; finished states have no way out.
ALLOWED_TRANSITIONS: dict[str | None, frozenset[str]] = {
    None: frozenset({"leased"}),
    "leased": frozenset({"running", "interrupted", "failed", "cancelled"}),
    "running": frozenset({"verifying", "waiting_human", "interrupted", "failed", "cancelled"}),
    "verifying": frozenset({"done", "running", "interrupted", "failed", "cancelled"}),
    # resume / release -> running; resume whose browser cannot be reopened -> interrupted
    "waiting_human": frozenset({"running", "interrupted", "failed", "cancelled"}),
    # an answer to an Ask left open by a quota stop resumes the same run
    "interrupted": frozenset({"running", "failed", "cancelled"}),
    "done": frozenset(),
    "failed": frozenset(),
    "cancelled": frozenset(),
}

_SCHEMA = """
CREATE TABLE runs (
  id TEXT PRIMARY KEY, order_id TEXT NOT NULL, project_id TEXT,
  order_json TEXT NOT NULL, variant_json TEXT NOT NULL, status TEXT,
  started_at TEXT NOT NULL, ended_at TEXT, parked_at TEXT,
  human_intervened INTEGER NOT NULL DEFAULT 0, taken_over INTEGER NOT NULL DEFAULT 0,
  checkpoint_json TEXT, evidence_json TEXT NOT NULL DEFAULT '[]',
  summary TEXT NOT NULL DEFAULT '', failure_json TEXT);
CREATE INDEX runs_status ON runs(status)
"""
_SCHEMA_V2 = """
ALTER TABLE runs ADD COLUMN asks_json TEXT NOT NULL DEFAULT '{}';
ALTER TABLE runs ADD COLUMN pending_json TEXT NOT NULL DEFAULT '[]'
"""
_SCHEMA_V3 = """
ALTER TABLE runs ADD COLUMN result_json TEXT;
ALTER TABLE runs ADD COLUMN retries INTEGER NOT NULL DEFAULT 0;
ALTER TABLE runs ADD COLUMN verify_rejects INTEGER NOT NULL DEFAULT 0;
ALTER TABLE runs ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0
"""


# Evidence is stored with isoformat timestamps (strict: `evidence_from_dict` parses them back).
def evidence_to_dict(ev: Evidence) -> dict:
    return {**ev, "captured_at": ev["captured_at"].isoformat()}


def result_to_json(res: RunResult) -> str:
    arts = [
        {**a, "evidence": [evidence_to_dict(e) for e in a["evidence"]]} for a in res["artifacts"]
    ]
    # not to_json: `default=str` writes a datetime as "date time", not isoformat's "dateTtime"
    return json.dumps({**res, "artifacts": arts}, ensure_ascii=False, default=str)


def result_from_json(raw: str) -> RunResult:
    d = json.loads(raw)
    d["artifacts"] = [
        {**a, "evidence": [evidence_from_dict(e) for e in a["evidence"]]} for a in d["artifacts"]
    ]
    return d


def evidence_from_dict(d: dict) -> Evidence:
    ev = Evidence(
        url=d.get("url"),
        screenshot=d.get("screenshot"),
        captured_at=datetime.fromisoformat(d["captured_at"]),
        text=d.get("text"),
    )
    if d.get("id"):
        ev["id"] = d["id"]
    return ev


class RunTable:
    """One row per Run; the WorkerPool is its only writer."""

    def __init__(self, db: Database, emit: Emitter, clock: Clock) -> None:
        self._db = db
        self._emit = emit
        self._clock = clock
        db.migrate("workers_runs_v1", _SCHEMA)
        db.migrate("workers_runs_v2", _SCHEMA_V2)
        db.migrate("workers_runs_v3", _SCHEMA_V3)

    # --- queries -------------------------------------------------------------------------

    def row(self, run_id: RunId):
        return self._db.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()

    @staticmethod
    def _view(r) -> WorkerRun:
        status = "running" if r["status"] == "leased" else r["status"]
        return WorkerRun(
            id=r["id"],
            order_id=r["order_id"],
            variant=json.loads(r["variant_json"]),
            status=status,
            started_at=datetime.fromisoformat(r["started_at"]),
            ended_at=datetime.fromisoformat(r["ended_at"]) if r["ended_at"] else None,
            human_intervened=bool(r["human_intervened"]),
        )

    def get(self, run_id: RunId) -> WorkerRun:
        r = self.row(run_id)
        if r is None:
            raise KeyError(run_id)
        return self._view(r)

    def active(self) -> list[WorkerRun]:
        marks = ",".join("?" * len(ACTIVE))
        rows = self._db.execute(
            f"SELECT * FROM runs WHERE status IN ({marks}) ORDER BY started_at, id", tuple(ACTIVE)
        ).fetchall()
        return [self._view(r) for r in rows]

    def project(self, run_id: RunId) -> ProjectId | None:
        r = self.row(run_id)
        return r["project_id"] if r else None

    def order(self, run_id: RunId) -> WorkOrder | None:
        """The WorkOrder a run executes (None for an unknown run)."""
        r = self.row(run_id)
        return order_from_json(r["order_json"]) if r else None

    def result(self, run_id: RunId) -> RunResult | None:
        """The stored RunResult of the run's last finished drive (None before one)."""
        r = self.row(run_id)
        return result_from_json(r["result_json"]) if r and r["result_json"] else None

    def checkpoint(self, run_id: RunId) -> dict | None:
        """Latest SessionCheckpoint of the run (raw material for HarnessTask.resume)."""
        r = self.row(run_id)
        return json.loads(r["checkpoint_json"]) if r and r["checkpoint_json"] else None

    def evidence(self, run_id: RunId) -> list[Evidence]:
        r = self.row(run_id)
        return [evidence_from_dict(d) for d in json.loads(r["evidence_json"])] if r else []

    # --- state machine ------------------------------------------------------------------

    def insert(
        self, run_id: RunId, order: WorkOrder, variant: Variant, *, started: datetime
    ) -> None:
        self._db.execute(
            "INSERT INTO runs(id, order_id, project_id, order_json, variant_json, started_at)"
            " VALUES (?,?,?,?,?,?)",
            (
                run_id,
                order["id"],
                order["project_id"],
                # not to_json: `default=str` writes a datetime as "date time", not "dateTtime"
                json.dumps(order, ensure_ascii=False, default=str),
                to_json(variant),
                started.isoformat(),
            ),
        )

    def transition(self, run_id: RunId, to: str) -> None:
        """The only writer of `runs.status`; emits `run.<to>`."""
        if to not in STATUSES:
            raise IllegalTransition(f"unknown status {to!r}")
        with self._db.transaction():
            r = self.row(run_id)
            if r is None:
                raise KeyError(run_id)
            frm = r["status"]
            if to not in ALLOWED_TRANSITIONS[frm]:
                raise IllegalTransition(f"run {run_id}: {frm} -> {to} not allowed")
            ended = self._clock.now().isoformat() if to in FINISHED else None
            self._db.execute(
                "UPDATE runs SET status=?, ended_at=COALESCE(?, ended_at) WHERE id=?",
                (to, ended, run_id),
            )
            self._emit(
                f"run.{to}", {"from": frm, "to": to}, run_id=run_id, project_id=r["project_id"]
            )

    def update(self, run_id: RunId, **cols: Any) -> None:
        """Update non-status columns of an unfinished run."""
        assert "status" not in cols
        with self._db.transaction():
            r = self.row(run_id)
            if r is None:
                raise KeyError(run_id)
            if r["status"] in FINISHED:
                raise RunFinished(f"run {run_id} is {r['status']} (immutable)")
            sets = ", ".join(f"{k}=?" for k in cols)
            self._db.execute(f"UPDATE runs SET {sets} WHERE id=?", (*cols.values(), run_id))

    def interrupt_orphans(self) -> list[RunId]:
        """running / leased / verifying Runs of a dead instance -> interrupted."""
        rows = self._db.execute(
            "SELECT id FROM runs WHERE status IN ('leased','running','verifying') "
            "ORDER BY started_at, id"
        ).fetchall()
        out: list[RunId] = []
        for r in rows:
            self.transition(r["id"], "interrupted")
            out.append(r["id"])
        return out

    def note_ask(self, run_id: RunId, ask_id: str, meta: dict) -> None:
        """Remember each Ask (kind, options, bound request) for the grant on resume, also
        after a reboot."""
        with contextlib.suppress(RunFinished, KeyError):
            r = self.row(run_id)
            asks = json.loads(r["asks_json"]) if r else {}
            asks[ask_id] = meta
            self.update(run_id, asks_json=to_json(asks))

    def ask_meta(self, run_id: RunId, ask_id: str) -> dict | None:
        """What `note_ask` remembered about an Ask (None if nothing)."""
        return json.loads(self.row(run_id)["asks_json"]).get(ask_id)

    def pending(self, run_id: RunId) -> list[str]:
        r = self.row(run_id)
        return list(json.loads(r["pending_json"])) if r else []

    def set_pending(self, run_id: RunId, parts: list[str]) -> None:
        self.update(run_id, pending_json=to_json(parts))

    def bump(self, run_id: RunId, col: str) -> int:
        """Increment a persisted counter (retries / verify_rejects / attempts)."""
        assert col in ("retries", "verify_rejects", "attempts")
        with self._db.transaction():
            n = self.row(run_id)[col] + 1
            self.update(run_id, **{col: n})
        return n
