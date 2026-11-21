"""Ports defined in shared so domain code never depends on orchestration."""

import glob
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol, TypedDict, TypeVar

from swarm.types import (
    AskId,
    BrowserHealth,
    BrowserId,
    CdpSession,
    GuardMode,
    Outbound,
    Project,
    ProjectId,
    RunId,
    WorkOrderId,
)

__all__ = [
    "ApprovalToken",
    "Brain",
    "BrainError",
    "BrainPurpose",
    "BrowserRuntime",
    "Guard",
    "Notifier",
    "Outbound",
    "ProjectLookup",
    "RequestDescriptor",
]

T = TypeVar("T")

BrainPurpose = Literal[
    "classify_message",
    "value_estimate",
    "event_worth",
    "report",
    "policy_match",
    "map_answer",
    "mine_task",
    "write_rubric",
    "judge",
]


class BrainError(Exception):
    """Raised when a Brain call fails (e.g. schema validation failed twice)."""


class Brain(Protocol):
    async def decide(
        self, *, purpose: BrainPurpose, prompt: str, schema: type[T], timeout_s: int = 60
    ) -> T: ...


class BrowserRuntime(Protocol):
    """CDP-family browser swap point. Harnesses only see the CdpSession."""

    id: BrowserId

    @property
    def profiles(self) -> Mapping[str, Path]:
        """Profile name -> user data dir; the pool rejects an order for any other name."""
        ...

    async def open_session(self, *, profile: str, purpose: str) -> CdpSession: ...

    async def close_session(self, session_id: str) -> None: ...  # never raises

    def session_alive(self, session_id: str) -> bool:
        """The pool asks before each harness turn and resume, and reopens the profile when the
        session died."""
        ...

    async def health(self) -> BrowserHealth: ...


class ProjectLookup(Protocol):
    """What the inbox needs of the project store: find a project by id."""

    def get(self, project_id: ProjectId) -> Project: ...


class Notifier(Protocol):
    """swarmd speaks first. `Outbound` (frozen signature) lives in swarm.types and
    is re-exported here. Fire-and-forget: the outbox owns delivery, ordering and retries."""

    async def send(self, msg: Outbound) -> None: ...  # enqueue to outbox; delivery is async


class RequestDescriptor(TypedDict):
    method: str
    host: str
    path: str  # path + "?" + query when there is a query; fragment dropped
    body_sha256: str
    fingerprint: str  # sha256(method, host, path, body)


@dataclass
class ApprovalToken:
    """One Ask = one request, consumed once, valid only for requests of `order_id`."""

    ask_id: AskId
    method: str
    host: str
    path_pattern: str  # fnmatch-style over path+query; an exact path matches only itself
    order_id: WorkOrderId
    used: bool = False

    @classmethod
    def from_descriptor(
        cls, ask_id: AskId, d: RequestDescriptor, *, order_id: WorkOrderId
    ) -> "ApprovalToken":
        return cls(
            ask_id=ask_id,
            method=d["method"],
            host=d["host"],
            path_pattern=glob.escape(d["path"]),
            order_id=order_id,
        )


class Guard(Protocol):
    """The request guard of one browser profile (the fail-closed gate on its network traffic).
    `grant` and `revoke` are synchronous: a token is decided without awaiting anything."""

    @property
    def connected(self) -> bool:
        """False once its CDP connection is gone: the owner must replace it."""
        ...

    async def arm(
        self,
        session: CdpSession,
        *,
        order_id: WorkOrderId,
        mode: GuardMode,
        run_id: RunId | None = None,
        dedupe_key: str | None = None,
    ) -> None: ...
    async def set_mode(self, session_id: str, mode: GuardMode) -> None: ...
    async def disarm(self, session_id: str) -> None: ...
    def grant(self, token: ApprovalToken) -> None: ...
    def revoke(self, ask_id: str) -> bool: ...
