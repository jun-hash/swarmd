"""Engine seam: types only."""

from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import NotRequired, Protocol, TypedDict

from swarm.types import EngineId, UsageSignal


class EngineTurn(TypedDict):
    prompt: str
    mcp_config: dict
    resume_id: str | None
    max_minutes: int  # runaway guard; the CLI has no turn limit, so the run enforces time
    allowed_tools: NotRequired[list[str]]  # tools the session may call without asking


@dataclass(frozen=True)
class Thinking:
    text: str


@dataclass(frozen=True)
class ToolCall:
    id: str
    name: str
    args: dict


@dataclass(frozen=True)
class ToolResult:
    id: str
    ok: bool
    preview: str
    ms: int


@dataclass(frozen=True)
class TurnUsage:
    input_tokens: int
    output_tokens: int
    cached_tokens: int
    cost_usd: float


@dataclass(frozen=True)
class QuotaSignal:
    signal: UsageSignal


@dataclass(frozen=True)
class Done:
    summary: str


@dataclass(frozen=True)
class Error:
    message: str


# an engine run that ended on a usage limit ends with `Error("quota:<kind>")`; the pool pauses
# the run instead of failing it
QUOTA_ERROR_PREFIX = "quota:"


def quota_error(kind: str) -> Error:
    return Error(message=f"{QUOTA_ERROR_PREFIX}{kind}")


EngineEvent = Thinking | ToolCall | ToolResult | TurnUsage | QuotaSignal | Done | Error


class EngineRun(Protocol):
    session_id: str | None

    def events(self) -> AsyncIterator[EngineEvent]: ...

    async def interrupt(self) -> None: ...


class Engine(Protocol):
    id: EngineId

    async def start(self, turn: EngineTurn) -> EngineRun: ...

    async def single_shot(self, *, prompt: str, json_schema: dict, timeout_s: int) -> dict: ...


class EngineError(Exception):
    """single_shot failure (CLI error, timeout, unparsable output)."""

    def __init__(self, message: str, signal: UsageSignal | None = None) -> None:
        super().__init__(message)
        self.signal = signal
