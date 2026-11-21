"""Coordinator: the router humans talk to.

It never works itself (no browser tools): it talks, assigns, reports and tidies memory.
This facade builds the parts and runs the agenda loop. The loop wakes on an event, when a
run ends, or every 60 s, and each tick runs these phases in order (a failing phase is
logged as `agenda.tick_error` and the rest still run):

  recover  items a dead process left running go back to ready      Dispatcher
  ingest   human edits of the project files                         ProjectStore
  inbox    Asks past their deadline close with the default          Resumes
  drain    closed Asks into the durable resume list, exactly once   Resumes
  held     items whose Ask closed unanswered are free again         Dispatcher
  dropped  lines a run ended before reading reach the human         Conversation
  reports  08:00 daily briefing, Friday 17:00 weekly                ReportScheduler
  agenda   choose_next rounds: respond (Conversation), resume (Resumes), queued human
           orders, advance and discover (Dispatcher), improve (Improver)

Durable state lives in CoordinatorRecords (the `coordinator_*` tables) and everything a
human hears goes through the Voice. Every task a part starts is tracked in one TaskSet,
so the daemon can `aclose()` them all on shutdown.
"""

import asyncio
import contextlib
import logging
from collections import deque
from collections.abc import Awaitable, Callable
from datetime import datetime, time
from typing import Any

from swarm.aio import TaskSet, pause
from swarm.clock import Clock
from swarm.coordinator.agenda import AgendaState, ProjectAgenda, choose_next
from swarm.coordinator.conversation import Conversation
from swarm.coordinator.dispatch import WORK_CAP, Dispatcher, RunControl
from swarm.coordinator.improve import Improver
from swarm.coordinator.records import CoordinatorRecords
from swarm.coordinator.reports import (
    ReportKind,
    ReportScheduler,
    build_daily,
    build_weekly,
    render_report,
)
from swarm.coordinator.resumes import Resumes
from swarm.coordinator.voice import Voice
from swarm.inbox.inbox import Inbox
from swarm.ports import Brain, Notifier
from swarm.projects.store import ProjectStore
from swarm.scheduler.ledger import QuotaLedger
from swarm.store.db import Database
from swarm.store.events import Emitter, EventLog
from swarm.text import error_text
from swarm.types import ChannelRef, Event, ProjectId, RunId, TellResult, Variant

log = logging.getLogger(__name__)

TICK_EVERY_S = 60.0
ACK_TIMEOUT_S = 10.0  # every front tool returns within 10 s
MAX_CHOICES = 16  # choose_next rounds per tick
SYNC_EVENTS = frozenset({"human.kanban_reorder", "human.kanban_archive"})  # no Brain call


class Coordinator:
    def __init__(
        self, *, db: Database, log: EventLog, projects: ProjectStore, inbox: Inbox,
        ledger: QuotaLedger, pool: RunControl, brain: Brain, notifier: Notifier, clock: Clock,
        general_channel: ChannelRef, default_profile: str, default_variant: Variant,
        work_cap: int = WORK_CAP, ack_timeout_s: float = ACK_TIMEOUT_S,
        tick_every_s: float = TICK_EVERY_S, daily_at: time = time(8, 0),
        weekly_at: time = time(17, 0), on_tick: Callable[[], None] | None = None,
    ) -> None:  # fmt: skip
        if default_variant["engine"] is None:
            raise ValueError("default_variant must name an engine")
        self._log = log
        self._projects = projects
        self._inbox = inbox
        self._ledger = ledger
        self._brain = brain
        self._clock = clock
        self._default_profile = default_profile  # improve orders: a real browser profile
        self._work_cap = work_cap
        self._tick_every_s = tick_every_s
        self._on_tick = on_tick  # after each completed tick (the daemon's heartbeat file)
        self.heartbeat_at: datetime = clock.now()
        self._wake = asyncio.Event()
        self._tasks = tasks = TaskSet(self._task_done)
        self._emit = emit = Emitter(log, clock, "coordinator")
        self.records = records = CoordinatorRecords(db, clock)
        self._events: deque[Event] = deque(records.held_events())  # awaiting a respond turn
        self.voice = voice = Voice(notifier, projects, general_channel, emit)
        # Improver launches its orders through the Dispatcher, and the Dispatcher hands each
        # discover result to the Improver: the lambda looks `self.improver` up at call time,
        # so the Dispatcher can be built first without holding the Improver.
        self.dispatch = dispatch = Dispatcher(
            records=records, pool=pool, ledger=ledger, projects=projects, inbox=inbox, voice=voice,
            emit=emit, tasks=tasks, clock=clock, wake=self._wake.set, work_cap=work_cap,
            default_variant=default_variant, on_discovered=lambda o, r: self.improver.triage(o, r),
        )  # fmt: skip
        self.resumes = Resumes(
            records=records, dispatch=dispatch, inbox=inbox, pool=pool, ledger=ledger,
            projects=projects, voice=voice, emit=emit, tasks=tasks, clock=clock, db=db,
        )  # fmt: skip
        self.conversation = Conversation(
            records=records, dispatch=dispatch, resumes=self.resumes, inbox=inbox, pool=pool,
            ledger=ledger, projects=projects, brain=brain, voice=voice, log=log, emit=emit,
            tasks=tasks, clock=clock, wake=self._wake.set, hold_event=self._hold_event,
            ack_timeout_s=ack_timeout_s,
        )  # fmt: skip
        self.improver = Improver(
            records=records, dispatch=dispatch, projects=projects, brain=brain, log=log,
            emit=emit, tasks=tasks, clock=clock,
        )  # fmt: skip
        self.report_schedule = ReportScheduler(
            kv=records.kv, projects=projects, voice=voice, emit=emit, tasks=tasks, clock=clock,
            general=general_channel, daily_at=daily_at, weekly_at=weekly_at, render=self._render,
        )  # fmt: skip

    # --- plumbing --------------------------------------------------------------------

    def _task_done(self, task: asyncio.Task) -> None:
        self._wake.set()  # a Run (or a respond turn) ended: tick now
        if task.cancelled() or task.exception() is None:
            return
        log.error("coordinator task %s failed", task.get_name(), exc_info=task.exception())
        with contextlib.suppress(Exception):
            self._emit("coordinator.task_error", task=task.get_name(), error=repr(task.exception()))

    def task_count(self) -> int:
        return len(self._tasks)

    async def drain(self) -> None:
        """Wait until every tracked task (and the tasks they start) has finished."""
        await self._tasks.drain()

    async def aclose(self) -> None:
        """Shutdown: cancel tracked tasks (runs are left interrupted by the pool) and gather."""
        await self._tasks.aclose()

    # --- loop ------------------------------------------------------------------------

    def _hold_event(self, ev: Event) -> None:
        """Keep a human event (on disk and in memory) until a respond turn handles it."""
        self._events.append(self.records.hold(ev))

    async def submit(self, ev: Event) -> None:
        """Any event wakes the loop; human events wait (on disk) for a respond turn."""
        if ev["kind"].startswith("human."):
            self._hold_event(ev)
        self._wake.set()

    async def agenda_loop(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            self._wake.clear()
            self.heartbeat_at = self._clock.now()
            try:
                await self.tick()
            except Exception as e:  # noqa: BLE001 - one bad tick never kills the loop
                self._tick_error("tick", e)
            else:
                if self._on_tick is not None:
                    try:
                        self._on_tick()
                    except Exception:  # noqa: BLE001 - a heartbeat hiccup never kills the loop
                        log.warning("on_tick hook failed", exc_info=True)
            self.heartbeat_at = self._clock.now()
            await pause(self._tick_every_s, stop, self._wake)

    def _tick_error(self, phase: str, e: Exception) -> None:
        log.error("agenda tick phase %s failed", phase, exc_info=e)
        with contextlib.suppress(Exception):
            self._emit("agenda.tick_error", phase=phase, error=error_text(e, 800))

    async def _phase(self, name: str, fn: Callable[..., Any], *args: Any) -> None:
        try:
            r = fn(*args)
            if isinstance(r, Awaitable):
                await r
        except Exception as e:  # noqa: BLE001
            self._tick_error(name, e)

    async def tick(self) -> None:
        now = self._clock.now()
        await self._phase("recover", self.dispatch.recover_items)
        await self._phase("ingest", self._projects.ingest_file_edits)
        await self._phase("inbox", self.resumes.inbox_tick, now)
        await self._phase("drain", self.resumes.drain_closed)
        await self._phase("held", self.dispatch.release_held)
        await self._phase("dropped", self.conversation.surface_dropped, now)
        await self._phase("reports", self.report_schedule.maybe_send, now)
        await self._phase("agenda", self._run_agenda, now)

    # --- agenda ----------------------------------------------------------------------

    def _auth_lost(self) -> bool:
        """Logged out (ledger auth_failed until auth_ok): no lease for anyone, so sleep."""
        return self._ledger.disabled(self.dispatch.engine)

    @staticmethod
    def _due(ev: Event, now: datetime) -> bool:
        nb = ev["payload"].get("not_before")
        return nb is None or datetime.fromisoformat(nb) <= now

    def _state(self, now: datetime, tried: set[str]) -> AgendaState:
        history = self.improver.history(now)
        last: dict[str, dict[ProjectId, datetime]] = {"advance": {}, "discover": {}}
        for e in history:
            kind, pid = e["payload"].get("kind"), e["project_id"]
            if kind in last and pid:
                last[kind][pid] = max(e["ts"], last[kind].get(pid, e["ts"]))
        reserved = self.records.reserved_items() | self.records.backoff_items(now)
        projects = [
            ProjectAgenda(
                id=p["id"],
                weight=p["weight"],
                backlog=[b for b in p["backlog"] if b["id"] not in reserved],
                last_advance_at=last["advance"].get(p["id"]),
                goal=p["goal"],
                default_profile=p["default_profile"],
            )
            for p in self._projects.list()
        ]
        return AgendaState(
            pending_events=[ev for ev in self._events if self._due(ev, now)],
            closed_asks=[o for o in self.resumes.due(now) if o["ask_id"] not in tried],
            projects=projects,
            running_count=self.dispatch.running_count(),
            # next_reset answers with *its* now when nothing cools: compare with a later read
            engine_available=self._ledger.next_reset(self.dispatch.engine) <= self._clock.now()
            and not self._auth_lost(),
            next_reset=self._ledger.next_reset(),
            last_discover_at=last["discover"],
            improve_rotation=self.improver.rotation(history),
            parallel_cap=self._work_cap,
            default_profile=self._default_profile,
        )

    async def _run_agenda(self, now: datetime) -> None:
        tried: set[str] = set()
        queued_done = False
        for _ in range(MAX_CHOICES + len(self._events)):
            choice = choose_next(self._state(now, tried), now)
            if choice.kind not in ("respond", "resume") and not queued_done:
                queued_done = True  # work a human asked for goes before agenda work
                if self.dispatch.dispatch_queued():
                    continue
            payload = choice.payload()
            self._emit("agenda.tick", payload, project_id=payload["project_id"])
            if choice.kind == "respond":
                ev = choice.event
                assert ev is not None
                self._events.remove(ev)
                turn = self.conversation.handled(ev["id"], self.conversation.respond(ev))
                if ev["kind"] in SYNC_EVENTS:  # applied now, so this tick already sees it
                    await turn
                else:
                    self._tasks.spawn(turn, f"respond:{ev['kind']}")
                continue
            if choice.kind == "resume":
                for o in choice.resume:
                    tried.add(o["ask_id"])
                    self.resumes.start(o)
                continue
            if choice.kind in ("advance", "discover") and self.dispatch.dispatch(choice):
                continue
            if choice.kind == "improve":
                await self.improver.improve(now)
            break  # one improve per tick; none / sleep / no lease: wait for the next tick

    # --- coordinator-owned Asks ----------------------------------------------------------

    def ask_project(self, run_id: RunId) -> ProjectId | None:
        """Project of a Coordinator-raised Ask (goal-change confirmation). The daemon wires
        `SqliteInbox(run_project=lambda r: coordinator.ask_project(r) or pool.run_project(r))`."""
        return self.resumes.ask_project(run_id)

    # --- what the front calls --------------------------------------------------------

    async def handle_tell(self, channel: ChannelRef, text: str) -> TellResult:
        """Ack within `ack_timeout_s`; slower work finishes in the background and its reply
        goes out through the Voice."""
        return await self.conversation.handle_tell(channel, text)

    async def status(self, channel: ChannelRef) -> str:
        return await self.conversation.status(channel)

    # --- reports -----------------------------------------------------------------------

    async def _render(self, kind: ReportKind, pid: ProjectId | None) -> str:
        build = build_daily if kind == "daily" else build_weekly
        facts = build(
            self._log, self._projects, self._inbox, self._ledger, self._clock.now(), project_id=pid
        )
        return await render_report(facts, self._brain)  # numbers from facts; template on error

    async def report(self, channel: ChannelRef, kind: ReportKind) -> str:
        """The daily or weekly report for the channel's project (all projects in General)."""
        project = self._projects.by_channel(channel)
        return await self._render(kind, project["id"] if project else None)
