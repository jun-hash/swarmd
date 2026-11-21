"""Harness seam: how browser work is done (own / browsercode / aside_exec / ...).

Every adapter speaks the same event stream. The stream ends with exactly one `Done` or `Error`
(an adapter synthesizes `Done` when its process ends cleanly without one). Quota signals from an
engine pass through unchanged (`QuotaSignal`, then `Error("quota:<kind>")`) so WorkerPool can map
them to `interrupted`.
"""

from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar, Literal, Protocol

from swarm.engine.base import (
    Done,
    Engine,
    Error,
    QuotaSignal,
    Thinking,
    ToolCall,
    ToolResult,
    TurnUsage,
)
from swarm.types import AskDraft, AskTicket, CdpSession, HarnessId, HarnessTask, RunId

__all__ = [
    "AskRaised",
    "ContextRollover",
    "Done",
    "Error",
    "FileOutput",
    "HarnessAdapter",
    "HarnessEvent",
    "HarnessHandle",
    "NotSupported",
    "QuotaSignal",
    "RunSink",
    "SessionCheckpoint",
    "SinkRecord",
    "Thinking",
    "ToolCall",
    "ToolResult",
    "TurnUsage",
    "UnknownHandle",
]

CAPABILITIES = frozenset({"resume", "steer", "screenshots", "file_output"})
HarnessHandle = RunId  # a live run is addressed by the run id its sink carries


@dataclass(frozen=True)
class AskRaised:
    """The worker sent a question to the human and is ending its turn (-> waiting_human)."""

    question: str
    irreversible: bool
    number: int | None = None  # "#n" topic number when the internal API returned one


@dataclass(frozen=True)
class FileOutput:
    path: str
    kind: Literal["screenshot", "file"]


@dataclass(frozen=True)
class ContextRollover:
    """`ctx.rollover`: a ContextPolicy cut the engine session and started a fresh one.
    Recorded to the sink only (raw material for policy comparison), never yielded."""

    kind: ClassVar[str] = "ctx.rollover"
    policy: str
    prev_session_id: str | None
    reason: str
    injected_chars: int
    injected_tokens: int


@dataclass(frozen=True)
class SessionCheckpoint:
    """`harness.checkpoint`: recorded to the sink right before every terminal event. Everything
    needed to continue later via `HarnessTask.resume` (steers not applied yet included)."""

    kind: ClassVar[str] = "harness.checkpoint"
    session_id: str | None
    notes: list[str]
    open_tabs: list[str]
    pending_steers: list[str]


HarnessEvent = (
    Thinking
    | ToolCall
    | ToolResult
    | AskRaised
    | FileOutput
    | TurnUsage
    | QuotaSignal
    | Done
    | Error
)
SinkRecord = HarnessEvent | ContextRollover | SessionCheckpoint


class NotSupported(Exception):
    """The adapter lacks the capability (e.g. `steer` not in `capabilities`)."""


class UnknownHandle(LookupError):
    """steer() for a run this adapter is not running (never started or already finished)."""


class RunSink(Protocol):
    """Implemented by WorkerPool. Questions to the human go only through `raise_ask`; output
    files go only under `output_dir`."""

    run_id: RunId
    output_dir: Path

    async def raise_ask(self, draft: AskDraft) -> AskTicket: ...

    def record(self, ev: SinkRecord) -> None: ...


class HarnessAdapter(Protocol):
    id: HarnessId
    needs_external_browser: bool  # aside_exec: False (brings its own browser)
    needs_engine: bool  # own only; the others embed an engine
    capabilities: frozenset[str]  # subset of CAPABILITIES

    def run(
        self,
        task: HarnessTask,
        *,
        browser: CdpSession | None,
        engine: Engine | None,
        sink: RunSink,
    ) -> AsyncIterator[HarnessEvent]: ...

    async def steer(self, handle: HarnessHandle, text: str) -> None: ...
