"""Binding a human approval to exactly one blocked request.

An approval / permission Ask is always irreversible and is bound, when it is raised, to the one
request the guard blocked for the order during the current harness turn. Only a human's
approving answer to that Ask, picked from its own options, lets that request through once.
"""

from collections.abc import Iterable
from typing import TypeGuard

from swarm.ports import RequestDescriptor
from swarm.types import AskDraft, AskOutcome, Event

APPROVAL_KINDS = frozenset({"approval", "permission"})
# the only options that approve a blocked request (exact, case-insensitive); anything else,
# including free text, never yields a token (a missed approval only makes the worker ask again)
APPROVING_OPTIONS = frozenset({"승인", "허용", "예", "approve", "allow", "yes"})
MACHINE_VIA = frozenset({"policy", "default"})


def harden_approval_draft(draft: AskDraft) -> tuple[AskDraft, list[str]]:
    """The draft without `forced`; approval kinds become irreversible with no default. Also
    the names of every field forced on the way, the MCP tool's own first."""
    forced = list(draft.get("forced") or [])  # what the MCP tool already overrode
    draft = AskDraft(**{k: v for k, v in draft.items() if k != "forced"})
    if draft["kind"] in APPROVAL_KINDS:
        if draft.get("default_after") is not None:
            forced.append("default_after")
        if not draft.get("irreversible"):
            forced.append("irreversible")
        draft = AskDraft(**{**draft, "irreversible": True, "default_after": None})
    return draft, forced


def bound_request(
    blocked: Iterable[Event], order_id: str
) -> tuple[RequestDescriptor | None, list[RequestDescriptor]]:
    """The request an approval Ask is about, and every candidate: bound only when exactly ONE
    distinct (method, host, path) of the order was blocked as `needs_token`."""
    found: dict[tuple, RequestDescriptor] = {}
    for e in blocked:
        p = e["payload"]
        d = p.get("descriptor")
        if p.get("order_id") == order_id and p.get("reason") == "needs_token" and d:
            found[(d["method"], d["host"], d["path"])] = d  # latest per request
    candidates = list(found.values())
    return (candidates[0] if len(candidates) == 1 else None), candidates


def is_approving_answer(meta: dict | None, outcome: AskOutcome) -> TypeGuard[dict]:
    """A human's answer to an approval / permission Ask that picked one of its own approving
    options. Never defaults, policy answers, free text or a choice outside the options."""
    if not isinstance(meta, dict) or meta.get("kind") not in APPROVAL_KINDS:
        return False
    via = outcome.get("via") or ""
    if outcome["status"] != "answered" or not via or via in MACHINE_VIA:
        return False
    choice = outcome.get("choice")
    if not isinstance(choice, str) or choice not in (meta.get("options") or []):
        return False
    return choice.strip().casefold() in APPROVING_OPTIONS
