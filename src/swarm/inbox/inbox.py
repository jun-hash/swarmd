"""Ask lifecycle: policy match, send-anchored deadlines, exactly-once close."""

import asyncio
import json
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Protocol

from pydantic import BaseModel

from swarm.clock import Clock
from swarm.ids import new_id
from swarm.ports import Brain, ProjectLookup
from swarm.store.db import Database
from swarm.store.events import Emitter, EventLog
from swarm.types import (
    AskDraft,
    AskId,
    AskOutcome,
    AskTicket,
    AskView,
    ChannelRef,
    ProjectId,
    RunId,
)

POLICY_CONFIDENCE = 0.8
POLICY_TIMEOUT_S = 15

_SCHEMA = """
CREATE TABLE asks (
  id TEXT PRIMARY KEY, run_id TEXT NOT NULL, project_id TEXT, channel TEXT NOT NULL,
  number INTEGER NOT NULL, draft_json TEXT NOT NULL, status TEXT NOT NULL,
  created_at TEXT NOT NULL, sent_at TEXT, closed_at TEXT, outcome_json TEXT,
  drained INTEGER NOT NULL DEFAULT 0, UNIQUE(channel, number));
CREATE INDEX asks_status ON asks(status);
CREATE INDEX asks_run ON asks(run_id)
"""


class AskAlreadyClosed(Exception):
    """The Ask was closed before this operation; an Ask closes exactly once."""


class PolicyMatch(BaseModel):
    policy_id: str | None = None
    choice: str | None = None
    confidence: float = 0.0


class Inbox(Protocol):
    """Delivery: `drain_closed()` is the only path that hands outcomes to the resumer.

    Policy-matched outcomes are delivered synchronously in the AskTicket and are marked drained
    at close, so they never come back from drain. `tick()` return values are informational
    only (e.g. a "proceeded with default" FYI); those outcomes stay undrained until
    `drain_closed()` delivers them exactly once. Expired outcomes (`expire_for_run`) are
    returned directly and never drained.
    """

    async def raise_ask(self, run_id: RunId, draft: AskDraft) -> AskTicket: ...
    async def answer(
        self, ask_id: AskId, *, choice: str, raw_text: str, via: ChannelRef
    ) -> AskOutcome: ...
    def open_asks(self, project_id: ProjectId | None = None) -> list[AskView]: ...
    async def tick(self, now: datetime) -> list[AskOutcome]: ...
    def drain_closed(self) -> list[AskOutcome]: ...
    def mark_sent(self, ask_id: AskId, at: datetime) -> None: ...
    def topic_number(self, ask_id: AskId) -> int: ...
    def expire_for_run(self, run_id: RunId) -> list[AskOutcome]: ...


def _draft_dump(draft: AskDraft, default_after: timedelta | None) -> str:
    d = dict(draft)
    d["default_after"] = default_after.total_seconds() if default_after is not None else None
    # not to_json: `default=str` writes a datetime as "date time", not isoformat's "dateTtime"
    return json.dumps(d, ensure_ascii=False, default=str)


def _policy_prompt(draft: AskDraft, policies: list[dict]) -> str:
    lines = [
        "사람에게 보낼 질문이 프로젝트 정책으로 이미 답해지는지 판단하라.",
        "정책이 질문에 명확히 적용될 때만 policy_id와 choice를 채우고, 아니면 null.",
        "confidence는 0~1.",
        "",
        f"질문: {draft['question']}",
        "선택지:",
        *[f"- {o}" for o in draft["options"]],
        "",
        "활성 정책:",
        *[f"- [{p['id']}] {p['text']}" for p in policies],
    ]
    return "\n".join(lines)


class SqliteInbox:
    def __init__(
        self,
        db: Database,
        log: EventLog,
        projects: ProjectLookup,
        brain: Brain,
        clock: Clock,
        *,
        run_project: Callable[[RunId], ProjectId | None],
        general_channel: ChannelRef,
    ) -> None:
        self._db = db
        self._projects = projects
        self._brain = brain
        self._clock = clock
        self._emit = Emitter(log, clock, "inbox")
        self._run_project = run_project
        self._general = general_channel
        db.migrate("inbox_v1", _SCHEMA)

    # --- helpers ------------------------------------------------------------------

    def _row(self, ask_id: AskId):
        r = self._db.execute("SELECT * FROM asks WHERE id=?", (ask_id,)).fetchone()
        if r is None:
            raise KeyError(ask_id)
        return r

    def _close(
        self,
        ask_id: AskId,
        *,
        status: str,
        choice: str | None,
        raw_text: str | None,
        via: str,
        only_if_due: datetime | None = None,
        drained: bool = False,
    ) -> AskOutcome | None:
        """Close once inside a transaction. With only_if_due, silently skip if not due/open."""
        with self._db.transaction():
            r = self._row(ask_id)
            if r["status"] != "open":
                if only_if_due is not None:
                    return None
                raise AskAlreadyClosed(ask_id)
            if only_if_due is not None and not self._is_due(r, only_if_due):
                return None
            outcome = AskOutcome(
                ask_id=ask_id,
                run_id=r["run_id"],
                status=status,  # type: ignore[typeddict-item]
                choice=choice,
                raw_text=raw_text,
                via=via,
            )
            self._db.execute(
                "UPDATE asks SET status=?, closed_at=?, outcome_json=?, drained=? WHERE id=?",
                (
                    status,
                    self._clock.now().isoformat(),
                    json.dumps(outcome, ensure_ascii=False),
                    1 if drained or status == "expired" else 0,
                    ask_id,
                ),
            )
        self._emit(
            f"ask.{status}",
            {"ask_id": ask_id, "choice": choice, "via": via},
            run_id=r["run_id"],
            project_id=r["project_id"],
        )
        return outcome

    @staticmethod
    def _deadline(r) -> datetime | None:
        if r["sent_at"] is None:
            return None
        secs = json.loads(r["draft_json"]).get("default_after")
        if secs is None:
            return None
        return datetime.fromisoformat(r["sent_at"]) + timedelta(seconds=secs)

    def _is_due(self, r, now: datetime) -> bool:
        dl = self._deadline(r)
        return dl is not None and dl <= now

    # --- raise and sweep ----------------------------------------------------------

    async def raise_ask(self, run_id: RunId, draft: AskDraft) -> AskTicket:
        project_id = self._run_project(run_id)
        project = self._projects.get(project_id) if project_id else None
        channel = project["channel"] if project else self._general
        default_after = None if draft["irreversible"] else draft["default_after"]
        default_ignored = False
        if default_after is not None and draft["recommended"] not in draft["options"]:
            default_after, default_ignored = None, True  # no valid recommended: wait for a human
            # (recommended None is never in options unless options contains None, which it cannot)
        ask_id = new_id("ask")
        with self._db.transaction():
            n = self._db.execute(
                "SELECT COALESCE(MAX(number), 0) + 1 FROM asks WHERE channel=?", (channel,)
            ).fetchone()[0]
            self._db.execute(
                "INSERT INTO asks(id, run_id, project_id, channel, number, draft_json, status,"
                " created_at) VALUES (?,?,?,?,?,?,'open',?)",
                (
                    ask_id,
                    run_id,
                    project_id,
                    channel,
                    n,
                    _draft_dump(draft, default_after),
                    self._clock.now().isoformat(),
                ),
            )
        self._emit(
            "ask.raised",
            {"ask_id": ask_id, "kind": draft["kind"], "number": n, "channel": channel},
            run_id=run_id,
            project_id=project_id,
        )
        if default_ignored:
            self._emit(
                "ask.default_ignored",
                {"ask_id": ask_id, "recommended": draft["recommended"]},
                run_id=run_id,
                project_id=project_id,
            )
        outcome = None
        if project and not draft["irreversible"]:
            outcome = await self._match_policy(ask_id, run_id, project_id, draft, project)
        return AskTicket(ask_id=ask_id, outcome=outcome)

    async def _match_policy(self, ask_id, run_id, project_id, draft, project) -> AskOutcome | None:
        active = [p for p in project["policies"] if p["active"]]
        if not active:
            return None
        try:
            m = await asyncio.wait_for(
                self._brain.decide(
                    purpose="policy_match",
                    prompt=_policy_prompt(draft, active),
                    schema=PolicyMatch,
                    timeout_s=POLICY_TIMEOUT_S,
                ),
                POLICY_TIMEOUT_S,
            )
        except Exception as e:  # BrainError, timeout, anything: fall through to the human
            reason = "timeout" if isinstance(e, TimeoutError) else f"{type(e).__name__}: {e}"
            self._emit(
                "ask.policy_match_failed",
                {"ask_id": ask_id, "reason": reason},
                run_id=run_id,
                project_id=project_id,
            )
            return None
        if (
            m.policy_id not in {p["id"] for p in active}
            or not m.choice
            or m.confidence < POLICY_CONFIDENCE
            or (draft["options"] and m.choice not in draft["options"])
        ):
            return None
        self._emit(
            "ask.policy_matched",
            {"ask_id": ask_id, "policy_id": m.policy_id, "confidence": m.confidence},
            run_id=run_id,
            project_id=project_id,
        )
        try:
            return self._close(
                ask_id,
                status="answered",
                choice=m.choice,
                raw_text=None,
                via="policy",
                drained=True,  # delivered synchronously in the AskTicket
            )
        except AskAlreadyClosed:  # a human answered while the Brain was thinking
            return None

    async def answer(
        self, ask_id: AskId, *, choice: str, raw_text: str, via: ChannelRef
    ) -> AskOutcome:
        out = self._close(ask_id, status="answered", choice=choice, raw_text=raw_text, via=via)
        assert out is not None
        return out

    def open_asks(self, project_id: ProjectId | None = None) -> list[AskView]:
        sql = "SELECT * FROM asks WHERE status='open'"
        params: tuple = ()
        if project_id is not None:
            sql += " AND project_id=?"
            params = (project_id,)
        views: list[AskView] = []
        for r in self._db.execute(sql + " ORDER BY created_at, number", params).fetchall():
            d = json.loads(r["draft_json"])
            secs = d.get("default_after")
            views.append(
                AskView(
                    ask_id=r["id"],
                    project_id=r["project_id"],
                    run_id=r["run_id"],
                    kind=d["kind"],
                    question=d["question"],
                    options=d["options"],
                    recommended=d["recommended"],
                    irreversible=d["irreversible"],
                    deadline=self._deadline(r),
                    default_after=timedelta(seconds=secs) if secs is not None else None,
                )
            )
        return views

    def mark_sent(self, ask_id: AskId, at: datetime) -> None:
        """Record when the Notifier actually sent it; the deadline counts from here (first only)."""
        with self._db.transaction():
            r = self._row(ask_id)
            if r["status"] != "open" or r["sent_at"] is not None:
                return
            self._db.execute("UPDATE asks SET sent_at=? WHERE id=?", (at.isoformat(), ask_id))
        self._emit("ask.sent", {"ask_id": ask_id}, run_id=r["run_id"], project_id=r["project_id"])

    def topic_number(self, ask_id: AskId) -> int:
        return self._row(ask_id)["number"]

    # --- tick and delivery --------------------------------------------------------

    async def tick(self, now: datetime) -> list[AskOutcome]:
        out: list[AskOutcome] = []
        rows = self._db.execute(
            "SELECT * FROM asks WHERE status='open' AND sent_at IS NOT NULL ORDER BY created_at"
        ).fetchall()
        for r in rows:
            if not self._is_due(r, now):
                continue
            d = json.loads(r["draft_json"])
            choice = d["recommended"]  # default_after is only stored with a valid recommended
            o = self._close(
                r["id"],
                status="defaulted",
                choice=choice,
                raw_text=None,
                via="default",
                only_if_due=now,
            )
            if o:
                out.append(o)
        return out

    def drain_closed(self) -> list[AskOutcome]:
        with self._db.transaction():
            rows = self._db.execute(
                "SELECT id, outcome_json FROM asks WHERE status!='open' AND drained=0"
                " ORDER BY closed_at, number"
            ).fetchall()
            for r in rows:
                self._db.execute("UPDATE asks SET drained=1 WHERE id=?", (r["id"],))
        return [json.loads(r["outcome_json"]) for r in rows]

    def expire_for_run(self, run_id: RunId) -> list[AskOutcome]:
        """Close a cancelled run's open Asks as expired (never resumed, never drained)."""
        out: list[AskOutcome] = []
        for r in self._db.execute(
            "SELECT id FROM asks WHERE run_id=? AND status='open'", (run_id,)
        ).fetchall():
            try:
                o = self._close(
                    r["id"], status="expired", choice=None, raw_text=None, via="expired"
                )
            except AskAlreadyClosed:
                continue
            if o:
                out.append(o)
        return out
