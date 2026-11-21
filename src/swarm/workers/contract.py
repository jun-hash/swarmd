"""What callers of the worker pool may rely on: its errors, the final run statuses,
WorkOrder validation and the strings the pool writes into a run's summary and a worker's
input."""

import json
from datetime import datetime

from swarm.engine.base import QUOTA_ERROR_PREFIX
from swarm.types import AskOutcome, WorkOrder

INTENT_MAX = 2_000
CRITERIA_MAX = 12
BUDGET_MINUTES = (1, 240)
NEEDS_PROJECT = frozenset({"respond", "advance", "discover"})
STUCK_REASON = "stuck"  # the watchdog cancels a stuck run with this reason
FINISHED = frozenset({"done", "failed", "cancelled"})  # a run in these never changes again


class InvalidOrder(ValueError):
    """WorkOrder violates the order contract (intent / done_criteria / budget / project_id)."""


class IncompatibleVariant(ValueError):
    """(harness, engine, browser) not in the compatibility table."""


class LeaseExpired(RuntimeError):
    """The lease is missing (harness needs an engine) or already expired."""


class UnknownProfile(LookupError):
    """order.profile is not a registered profile of the variant's browser."""


class IllegalTransition(RuntimeError):
    """A status change not in ALLOWED_TRANSITIONS (finished runs are immutable)."""


class RunFinished(RuntimeError):
    """Write to a finished (immutable) run."""


def order_from_json(raw: str) -> WorkOrder:
    """order_json back to a WorkOrder (`due` was stored as an ISO string)."""
    o = json.loads(raw)
    if isinstance(o.get("due"), str):
        o["due"] = datetime.fromisoformat(o["due"])
    return o


def render_outcome(outcome: AskOutcome) -> str:
    choice = outcome["choice"] or outcome["raw_text"] or ""
    if outcome["status"] == "defaulted":
        return f"기본값으로 진행: {choice}"
    return f"사람 답: {choice}"


def steer_line(text: str) -> str:
    return f"사람 지시: {text}"


def validate_order(order: WorkOrder) -> None:
    """Input checks; raises InvalidOrder."""
    intent = order.get("intent")
    if not isinstance(intent, str) or not intent.strip() or len(intent) > INTENT_MAX:
        raise InvalidOrder(f"intent must be 1~{INTENT_MAX} chars")
    crit = order.get("done_criteria")
    if not isinstance(crit, list) or not 1 <= len(crit) <= CRITERIA_MAX:
        raise InvalidOrder(f"done_criteria must have 1~{CRITERIA_MAX} items")
    if any(not isinstance(c, str) or not c.strip() for c in crit):
        raise InvalidOrder("done_criteria items must be non-empty sentences")
    minutes = (order.get("budget") or {}).get("max_minutes")
    lo, hi = BUDGET_MINUTES
    if isinstance(minutes, bool) or not isinstance(minutes, int) or not lo <= minutes <= hi:
        raise InvalidOrder(f"budget.max_minutes must be {lo}~{hi}")
    if order.get("kind") in NEEDS_PROJECT and not order.get("project_id"):
        raise InvalidOrder(f"{order.get('kind')} order needs a project_id")
    if order.get("action_mode") not in ("guarded", "dry_run"):
        raise InvalidOrder("action_mode must be guarded or dry_run")
    if not order.get("id"):
        raise InvalidOrder("order id missing")


def dedupe_key(order: WorkOrder) -> str:
    """ActionGuard "already executed" scope: the backlog item, so a re-dispatched
    item (new order id after an interrupt or a reboot) never repeats a request its earlier
    run already sent; orders without an item fall back to their own id."""
    item = order.get("backlog_item_id")
    return f"item:{item}" if item else order["id"]


def cancelled_summary(reason: str) -> str:
    return f"cancelled: {reason}"


_PAUSED = "paused: "


def paused_summary(engine_error: str) -> str:
    """The summary of a run an engine error paused (`interrupted`, resumable later)."""
    return _PAUSED + engine_error


def is_quota_pause(summary: str) -> bool:
    """A run summary the pool wrote when an engine usage limit paused the run."""
    return summary.startswith(paused_summary(QUOTA_ERROR_PREFIX))
