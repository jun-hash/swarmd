"""Minimal quota gate: cooling until reset, auth disable, parallel cap.

No budgeting: no remaining-quota estimation, no per-purpose thresholds. `purpose` is stored
on the lease for logging only.
"""

from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Literal, Protocol

from swarm.clock import Clock
from swarm.ids import new_id
from swarm.store.db import Database
from swarm.store.events import Emitter, EventLog
from swarm.types import EngineId, OrderKind, QuotaLease, UsageSignal

DEFAULT_COOLING = timedelta(hours=1)
MIN_COOLING = timedelta(minutes=1)  # a reset_at already past still rests the engine this long
LEASE_GRACE = timedelta(minutes=10)
DEFAULT_MAX_MINUTES = 60

_SCHEMA = """
CREATE TABLE quota_engines (
  engine TEXT PRIMARY KEY, cooling_until TEXT, disabled INTEGER NOT NULL DEFAULT 0);
CREATE TABLE quota_leases (
  id TEXT PRIMARY KEY, engine TEXT NOT NULL, purpose TEXT NOT NULL,
  instance_id TEXT NOT NULL, expires_at TEXT NOT NULL);
CREATE INDEX quota_leases_engine ON quota_leases(engine)
"""


class QuotaLedger(Protocol):
    def lease(
        self,
        *,
        engine_pref: Sequence[EngineId],
        purpose: OrderKind | Literal["verify"],
        max_minutes: int = DEFAULT_MAX_MINUTES,
    ) -> QuotaLease | None: ...
    def release(self, lease: QuotaLease) -> None: ...
    def observe(self, *, engine: EngineId, signal: UsageSignal) -> None: ...
    def next_reset(self, engine: EngineId | None = None) -> datetime: ...
    def disabled(self, engine: EngineId) -> bool: ...


class SqliteQuotaLedger:
    def __init__(
        self,
        db: Database,
        log: EventLog,
        clock: Clock,
        instance_id: str,
        caps: dict[EngineId, int] | None = None,
    ) -> None:
        self._db = db
        self._clock = clock
        self._emit = Emitter(log, clock, "scheduler")
        self._instance = instance_id
        self._caps = dict(caps) if caps is not None else {"claude": 3}
        db.migrate("quota_ledger_v1", _SCHEMA)
        self._recover_boot()

    def _recover_boot(self) -> None:
        with self._db.transaction():
            rows = self._db.execute(
                "SELECT id, engine FROM quota_leases WHERE instance_id != ?", (self._instance,)
            ).fetchall()
            for r in rows:
                self._db.execute("DELETE FROM quota_leases WHERE id=?", (r["id"],))
                self._emit(
                    "scheduler.release", {"lease_id": r["id"], "engine": r["engine"], "stale": True}
                )

    def _state(self, engine: EngineId) -> tuple[datetime | None, bool]:
        r = self._db.execute(
            "SELECT cooling_until, disabled FROM quota_engines WHERE engine=?", (engine,)
        ).fetchone()
        if r is None:
            return None, False
        cu = datetime.fromisoformat(r["cooling_until"]) if r["cooling_until"] else None
        return cu, bool(r["disabled"])

    def _reclaim_expired(self) -> None:
        now = self._clock.now()
        for r in self._db.execute("SELECT * FROM quota_leases").fetchall():
            if datetime.fromisoformat(r["expires_at"]) <= now:
                self._db.execute("DELETE FROM quota_leases WHERE id=?", (r["id"],))
                self._emit(
                    "scheduler.lease_expired",
                    {"lease_id": r["id"], "engine": r["engine"], "purpose": r["purpose"]},
                )

    def lease(
        self,
        *,
        engine_pref: Sequence[EngineId],
        purpose: OrderKind | Literal["verify"],
        max_minutes: int = DEFAULT_MAX_MINUTES,
    ) -> QuotaLease | None:
        if not engine_pref:
            raise ValueError("engine_pref must not be empty")
        with self._db.transaction():
            self._reclaim_expired()
            now = self._clock.now()
            for engine in engine_pref:
                cap = self._caps.get(engine)
                if cap is None:
                    continue
                cooling_until, disabled = self._state(engine)
                if disabled or (cooling_until is not None and cooling_until > now):
                    continue
                active = self._db.execute(
                    "SELECT count(*) FROM quota_leases WHERE engine=?", (engine,)
                ).fetchone()[0]
                if active >= cap:
                    continue
                lease = QuotaLease(
                    id=new_id("lse"),
                    engine=engine,
                    purpose=purpose,
                    expires_at=now + timedelta(minutes=max_minutes) + LEASE_GRACE,
                )
                self._db.execute(
                    "INSERT INTO quota_leases(id, engine, purpose, instance_id, expires_at) "
                    "VALUES (?,?,?,?,?)",
                    (
                        lease["id"],
                        engine,
                        purpose,
                        self._instance,
                        lease["expires_at"].isoformat(),
                    ),
                )
                self._emit(
                    "scheduler.lease",
                    {"lease_id": lease["id"], "engine": engine, "purpose": purpose},
                )
                return lease
            return None

    def release(self, lease: QuotaLease) -> None:
        with self._db.transaction():
            cur = self._db.execute("DELETE FROM quota_leases WHERE id=?", (lease["id"],))
            if cur.rowcount:
                self._emit(
                    "scheduler.release", {"lease_id": lease["id"], "engine": lease["engine"]}
                )

    def observe(self, *, engine: EngineId, signal: UsageSignal) -> None:
        with self._db.transaction():
            cooling_until, disabled = self._state(engine)
            if signal["kind"] == "limit_hit":
                until = signal["reset_at"] or signal["at"] + DEFAULT_COOLING
                until = max(until, signal["at"] + MIN_COOLING)  # never a cooling already over
                if cooling_until is not None and cooling_until > until:
                    until = cooling_until  # never shorten an existing cooling period
                self._upsert(engine, until, disabled)
                self._emit(
                    "engine.quota_exhausted",
                    {"engine": engine, "reset_at": until.isoformat()},
                )
            elif signal["kind"] == "auth_failed":
                self._upsert(engine, cooling_until, True)
                if not disabled:
                    self._emit("engine.auth_failed", {"engine": engine})
            elif signal["kind"] == "auth_ok":
                self._upsert(engine, cooling_until, False)
                if disabled:
                    self._emit("engine.auth_ok", {"engine": engine})

    def _upsert(self, engine: EngineId, cooling_until: datetime | None, disabled: bool) -> None:
        self._db.execute(
            "INSERT INTO quota_engines(engine, cooling_until, disabled) VALUES (?,?,?) "
            "ON CONFLICT(engine) DO UPDATE SET cooling_until=excluded.cooling_until, "
            "disabled=excluded.disabled",
            (engine, cooling_until.isoformat() if cooling_until else None, int(disabled)),
        )

    def disabled(self, engine: EngineId) -> bool:
        """True while an auth failure keeps `engine` from leasing (until auth_ok)."""
        return self._state(engine)[1]

    def next_reset(self, engine: EngineId | None = None) -> datetime:
        now = self._clock.now()
        engines = (
            [engine]
            if engine is not None
            else [r["engine"] for r in self._db.execute("SELECT engine FROM quota_engines")]
        )
        pending = []
        for e in engines:
            cu, _ = self._state(e)
            if cu is not None and cu > now:
                pending.append(cu)
        return min(pending) if pending else now
