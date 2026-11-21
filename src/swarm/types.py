"""Value types shared by every layer. Plain TypedDicts, no behavior."""

from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal, NotRequired, TypedDict

ChannelRef = str  # "telegram:<chat_id>[:<thread_id>]"
ProjectId = str
RunId = str
AskId = str
WorkOrderId = str
BacklogItemId = str
PolicyId = str
KrId = str
EventId = str
BlobRef = str  # "blob_<id>.<ext>"
EngineId = str  # "claude" | "codex"
BrowserId = str  # "chrome" | "helium" | "aside"
HarnessId = str  # "own" | "browsercode" | "aside_exec" | "hermes_exec"

OrderKind = Literal["respond", "advance", "discover", "improve", "eval"]
SteerMode = Literal["live", "at_tool_boundary"]
GuardMode = Literal["guarded", "dry_run", "off"]
BacklogStatus = Literal[
    "triage", "todo", "ready", "running", "blocked", "review", "done", "archived", "dropped"
]


# --- evidence / memory --------------------------------------------------------------


class Evidence(TypedDict):
    """Proof of a claim: URL, screenshot path, time, extracted text."""

    url: str | None
    screenshot: str | None  # blob path / ref
    captured_at: datetime
    text: str | None
    id: NotRequired[str]  # "ev_…" when posted through the internal API


class Learning(TypedDict):
    text: str
    evidence: list[Evidence]


class FailedAttempt(TypedDict):
    """What was tried and why it failed; force-injected into similar future work."""

    what: str
    why: str
    run_id: RunId | None


class MemoryEntry(TypedDict):
    key: str
    text: str
    updated_at: datetime


class MemoryBrief(TypedDict):
    """Budgeted summary injected into workers (global 25% / project 50% / failures 25%)."""

    text: str
    refs: list[str]  # ids of items spilled out of `text` (Inline & Spill)
    tokens: int  # len(text) // 3


# --- project ------------------------------------------------------------------------


class KR(TypedDict):
    id: KrId
    metric: str
    target: float
    current: float
    source: str


class Goal(TypedDict):
    objective: str
    key_results: list[KR]


class Policy(TypedDict):
    id: PolicyId
    text: str
    active: bool  # policies are only added or deactivated, never edited
    created_at: datetime
    source_ask: NotRequired[str | None]


class BacklogItem(TypedDict):
    id: BacklogItemId
    title: str
    intent: str
    done_criteria: list[str]
    status: BacklogStatus
    priority: int
    value: int  # 1~5


class ProjectMemory(TypedDict):
    context: str
    learnings: list[Learning]
    failed_attempts: list[FailedAttempt]


class ArtifactRef(TypedDict):
    id: str
    title: str
    path: str


class WatchSpec(TypedDict):
    """Watch target (page diff, RSS, keyword). Not implemented: the list stays empty."""

    kind: Literal["page_diff", "rss", "keyword"]
    target: str


class Budget(TypedDict):
    max_minutes: int  # 1~240, default 60; runaway guard only


class Variant(TypedDict):
    harness: HarnessId
    engine: EngineId | None
    browser: BrowserId
    context_policy: str
    model: str | None


class Project(TypedDict):
    id: ProjectId
    slug: str
    name: str
    goal: Goal
    strategy: str
    policies: list[Policy]
    autonomy: int  # 1~5
    memory: ProjectMemory
    backlog: list[BacklogItem]
    artifacts: list[ArtifactRef]
    channel: ChannelRef
    board: str
    default_profile: str
    default_variant: Variant
    weight: int  # 1~3, default 2
    watches: list[WatchSpec]


# --- work ---------------------------------------------------------------------------


class WorkOrder(TypedDict):
    id: WorkOrderId
    project_id: ProjectId | None
    kind: OrderKind
    intent: str
    done_criteria: list[str]
    budget: Budget
    profile: str
    priority: float
    value: int
    due: datetime | None
    backlog_item_id: BacklogItemId | None
    action_mode: Literal["guarded", "dry_run"]


class Cost(TypedDict):
    turns: int
    minutes: float
    engine_usage: dict  # measured metric, not a budget


class Artifact(TypedDict):
    id: str
    title: str
    path: str
    status: Literal["partial", "reported", "verified"]
    evidence: list[Evidence]
    verifier_run_id: RunId | None
    # verifier ran on the worker's engine (there is one engine: always true)
    verifier_same_engine: NotRequired[bool]


class RunResult(TypedDict):
    run_id: RunId
    status: Literal["done", "waiting_human", "interrupted", "failed", "cancelled"]
    artifacts: list[Artifact]
    summary: str  # <= 600 chars
    cost: Cost
    failure: FailedAttempt | None


class WorkerRun(TypedDict):
    """One Run of exactly one WorkOrder with exactly one Variant."""

    id: RunId
    order_id: WorkOrderId
    variant: Variant
    status: Literal[
        "running", "verifying", "waiting_human", "done", "interrupted", "failed", "cancelled"
    ]
    started_at: datetime
    ended_at: datetime | None
    human_intervened: bool


class HarnessResume(TypedDict):
    """Continue earlier work (answered Ask, takeover release, reopened browser). `session_id`
    None -> a fresh engine session primed with notes/open_tabs; `message` is what to say now."""

    session_id: str | None
    message: str
    notes: list[str]
    open_tabs: list[str]


class HarnessTask(TypedDict):
    instruction: str
    done_criteria: list[str]
    start_url: str | None
    memory_brief: str
    policies: list[str]
    budget: Budget
    action_mode: Literal["guarded", "dry_run"]
    recent_failures: NotRequired[list[str]]  # "what -> why" lines of similar failed attempts
    resume: NotRequired[HarnessResume]  # from the last SessionCheckpoint of an earlier run


class CdpSession(TypedDict):
    """A dedicated browser target. `guard` is "off" until ActionGuard.arm() succeeds and then
    tracks the guard's mode: callers must arm the session BEFORE handing `cdp_url` /
    `target_id` to a harness."""

    id: str
    cdp_url: str
    target_id: str
    profile: str
    capabilities: frozenset[str]
    guard: GuardMode


class BrowserHealth(TypedDict):
    browser: BrowserId
    ok: bool
    profiles: dict[str, bool]  # running profile -> CDP answers
    sessions: int
    version: str | None  # e.g. "Chrome/156.0.8078.80"


class Observation(TypedDict):
    """Compressed view of a page handed to the engine (<= 8,000 chars)."""

    url: str
    title: str
    text: str


class TakeoverInfo(TypedDict):
    run_id: RunId
    profile: str
    cdp_url: str
    target_id: str
    url: NotRequired[str | None]  # last known URL of the worker's tab
    instructions: NotRequired[str]  # what to tell the human


# --- inbox --------------------------------------------------------------------------


class AskDraft(TypedDict):
    kind: Literal["decision", "approval", "permission", "takeover"]
    question: str
    options: list[str]
    recommended: str | None
    default_after: timedelta | None  # must be None when irreversible
    irreversible: bool
    evidence: list[Evidence]
    forced: NotRequired[list[str]]  # fields the MCP overrode (approval kinds); never stored


class AskOutcome(TypedDict):
    ask_id: AskId
    run_id: RunId
    status: Literal["answered", "defaulted", "expired"]
    choice: str | None
    raw_text: str | None
    via: str  # "telegram:..." | "policy" | "default"


class AskTicket(TypedDict):
    ask_id: AskId
    outcome: AskOutcome | None  # set when a policy closed it immediately
    # topic number shown to the human ("#n"), filled in by the internal API
    number: NotRequired[int]


class AskView(TypedDict):
    ask_id: AskId
    project_id: ProjectId | None
    run_id: RunId
    kind: Literal["decision", "approval", "permission", "takeover"]
    question: str
    options: list[str]
    recommended: str | None
    irreversible: bool
    deadline: datetime | None  # sent_at + default_after; None until sent
    default_after: NotRequired[timedelta | None]


class ProjectView(TypedDict):
    id: ProjectId
    slug: str
    name: str
    channel: ChannelRef
    objective: str
    open_asks: int


# --- front / events -----------------------------------------------------------------


class TellResult(TypedDict):
    ack: str
    project_id: ProjectId | None
    order_id: WorkOrderId | None
    closed_ask_id: AskId | None


class Outbound(TypedDict):
    channel: ChannelRef
    kind: Literal["ask", "report", "fyi", "ack"]
    text: str  # <= 3,500 chars, first line = type + conclusion
    ref_id: str | None
    urgent: bool


class Event(TypedDict):
    id: EventId
    ts: datetime
    kind: str  # "area.event", lowercase
    actor: str
    project_id: ProjectId | None
    run_id: RunId | None
    payload: dict  # <= 16KB, larger goes to a blob
    refs: list[str]


CardStatus = Literal["triage", "todo", "ready", "running", "blocked", "review", "done", "archived"]


class CardView(TypedDict):
    """One card on the Hermes kanban. `key` = backlog_item_id or run_id, also the
    Hermes idempotency key; `board` = project slug; body <= 4,000 chars."""

    key: str
    board: str
    title: str
    body: str
    status: CardStatus
    priority: int
    links: list[str]
    attachments: list[Path]


class KanbanChange(TypedDict):
    """A human edit on the Hermes board (echoes of our own writes never become one)."""

    card_key: str
    kind: Literal["moved", "reordered", "archived", "commented"]
    to_status: CardStatus | None
    priority: int | None
    comment: str | None


# --- scheduler ----------------------------------------------------------------------


class QuotaLease(TypedDict):
    id: str
    engine: EngineId
    purpose: str
    expires_at: datetime  # max_minutes + 10 min


class UsageSignal(TypedDict):
    kind: Literal["limit_hit", "auth_failed", "auth_ok"]
    reset_at: datetime | None
    at: datetime
