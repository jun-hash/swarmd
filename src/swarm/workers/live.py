"""The in-memory side of a Run: what the runs table cannot hold."""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from swarm.engine.base import Engine
from swarm.harness.base import FileOutput, HarnessAdapter, SinkRecord
from swarm.ports import BrowserRuntime, Guard
from swarm.types import (
    AskDraft,
    AskTicket,
    CdpSession,
    HarnessTask,
    QuotaLease,
    RunId,
    Variant,
    WorkOrder,
)


@dataclass(frozen=True)
class NextTurn:
    """The driver goes on with another harness turn (retry, verify reject)."""

    task: HarnessTask


@dataclass
class LiveRun:
    """In-memory side of a Run (what the table cannot hold: tasks, adapter, session).

    The pool keeps one per run it drives or has parked, next to the DB status machine. The
    fields below the fixed identity form a small state machine of their own; each is written
    only where noted (all in `WorkerPool`).

    Session and guard:
      `session` is set by `_open_session`; cleared by `_close_session` (teardown, park
      timeout) and `_drop_dead_session` (browser crash). `guard` is set by `_arm` and
      `_grant_approval`; cleared with the session in `_drop_dead_session`. `armed` turns
      True at the end of `_arm` and False whenever `session` is replaced or cleared.
      `granted` lists ask ids holding a token on the guard: appended by `_grant_approval`,
      emptied by `_revoke_grants` when a drive ends (`_finish`), trimmed by `_drop_grant`.

    Tasks and cancellation (a task handle is never cleared unless noted; check `done()`):
      `driver` drives the harness turns: set by `run` and `_launch` (resume, release).
      `tearing_down` is the disarm+close in flight: set by `_teardown`, cleared by
      `_close_session` when it ends. `park_timer` closes a parked session after 2 h: set
      by `_park`, cancelled and cleared by `_stop_park_timer`. `releaser` keeps the drive
      started by `release()` alive. `canceller` is the cancel cleanup of a run with no live
      driver, set by `_cancel_locked`. `cancel_reason` is set by `cancel` before it waits
      for the run's lock (taken back if that wait is cancelled) and `cancel_started` by
      `_cancel_locked` when the one cleanup starts. `taking_over` is True only while
      `_pause_driver` waits for the driver, so it parks the run instead of interrupting it.
      `lease` is dropped by `_release_lease` and replaced by `_launch`.

    Per turn (reset or rolled over by `_consume`, `_prepare_resume`, `_settle_pending`):
      `asked` is set when the harness raises an Ask (`_on_event`) and cleared on resume or
      when an error closes the turn's Asks. `segment_started` and `segment_checkpoints` are
      stamped by `_consume` at each turn's start; `checkpoints` counts SessionCheckpoints
      (`_on_record`). `delivering` holds the pending lines this turn's message carries
      (`_resume_task`), dropped by `_settle_pending` once a Done shows they arrived.
      `files`, `turns` and `usage` accumulate over the whole run (`_on_event`).
    """

    run_id: RunId
    order: WorkOrder
    variant: Variant
    adapter: HarnessAdapter
    engine: Engine | None
    browser: BrowserRuntime
    output_dir: Path
    started: datetime
    lease: QuotaLease | None
    engine_id: str | None
    # --- session and guard
    guard: Guard | None = None
    session: CdpSession | None = None
    armed: bool = False  # the guard covers `session`
    granted: list[str] = field(default_factory=list)  # ask ids with a token on the guard
    # --- tasks and cancellation
    driver: asyncio.Task | None = None
    canceller: asyncio.Task | None = None  # cancel cleanup of a run with no live driver
    tearing_down: asyncio.Task | None = None  # disarm+close in flight (park timer/resume wait)
    park_timer: asyncio.Task | None = None
    releaser: asyncio.Task | None = None  # background drive started by release() (kept alive)
    cancel_reason: str | None = None  # set by cancel() before it waits for the run's lock
    cancel_started: bool = False  # the one cleanup has been started
    taking_over: bool = False  # takeover() interrupted the driver: park instead of interrupt
    # --- per turn, and totals over the run
    asked: bool = False
    segment_started: datetime | None = None  # start of the current harness turn
    checkpoints: int = 0  # SessionCheckpoints recorded so far (freshness of the stored one)
    segment_checkpoints: int = 0  # value of `checkpoints` when the current turn started
    delivering: list[str] = field(default_factory=list)  # pending lines in this turn's message
    files: list[FileOutput] = field(default_factory=list)
    turns: int = 0
    usage: dict[str, float] = field(
        default_factory=lambda: {
            "input_tokens": 0,
            "output_tokens": 0,
            "cached_tokens": 0,
            "cost_usd": 0.0,
        }
    )


class PoolSink:
    """RunSink handed to the harness: asks go to the Inbox, records go to the EventLog."""

    def __init__(
        self,
        run_id: RunId,
        output_dir: Path,
        raise_ask: Callable[[AskDraft], Awaitable[AskTicket]],
        record: Callable[[SinkRecord], None],
    ) -> None:
        self._raise_ask = raise_ask
        self._record = record
        self.run_id = run_id
        self.output_dir = output_dir

    async def raise_ask(self, draft: AskDraft) -> AskTicket:
        return await self._raise_ask(draft)

    def record(self, ev: SinkRecord) -> None:
        self._record(ev)
