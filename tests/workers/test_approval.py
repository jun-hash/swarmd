"""Binding a human approval to exactly one blocked request (pure rules, no pool)."""

from datetime import datetime, timedelta

from swarm.types import AskDraft, AskOutcome, Event
from swarm.workers.approval import bound_request, harden_approval_draft, is_approving_answer

POST = {
    "method": "POST",
    "host": "shop.test",
    "path": "/order",
    "body_sha256": "a",
    "fingerprint": "1",
}
POST_AGAIN = {**POST, "body_sha256": "b", "fingerprint": "2"}  # same request, other body
BEACON = {**POST, "path": "/beacon", "fingerprint": "3"}


def blocked(d, *, order_id="ord_1", reason="needs_token") -> Event:
    return Event(
        id="",
        ts=datetime(2026, 11, 6, 9, 0),
        kind="guard.blocked",
        actor="guard",
        project_id=None,
        run_id="run_1",
        payload={"order_id": order_id, "mode": "guarded", "reason": reason, "descriptor": d},
        refs=[],
    )


def test_bound_request_needs_exactly_one_distinct_request():
    assert bound_request([], "ord_1") == (None, [])
    noise = [blocked(BEACON, order_id="ord_2"), blocked(BEACON, reason="denied"), blocked(None)]
    assert bound_request(noise, "ord_1") == (None, [])
    # one distinct (method, host, path): the latest descriptor of it is bound
    assert bound_request([blocked(POST), blocked(POST_AGAIN), *noise], "ord_1") == (
        POST_AGAIN,
        [POST_AGAIN],
    )
    assert bound_request([blocked(POST), blocked(BEACON)], "ord_1") == (None, [POST, BEACON])


def outcome(choice="승인", *, status="answered", via="telegram:-100:1") -> AskOutcome:
    return AskOutcome(
        ask_id="ask_1", run_id="run_1", status=status, choice=choice, raw_text=None, via=via
    )


def test_is_approving_answer_rejects_defaults_policy_free_text_and_foreign_options():
    meta = {"kind": "approval", "options": ["승인", "거절"], "descriptor": POST}
    assert is_approving_answer(meta, outcome())
    assert is_approving_answer({**meta, "kind": "permission"}, outcome())
    assert is_approving_answer({**meta, "options": ["Yes", "No"]}, outcome("Yes"))
    assert not is_approving_answer(None, outcome())
    assert not is_approving_answer({**meta, "kind": "decision"}, outcome())
    assert not is_approving_answer(meta, outcome(status="defaulted"))
    assert not is_approving_answer(meta, outcome(via="default"))
    assert not is_approving_answer(meta, outcome(via="policy"))
    assert not is_approving_answer(meta, outcome(via=""))
    assert not is_approving_answer(meta, outcome(None))  # free text
    assert not is_approving_answer(meta, outcome("거절"))
    assert not is_approving_answer(meta, outcome("허용"))  # approving, but not one of its options


def draft(kind="approval", **kw) -> AskDraft:
    d = AskDraft(
        kind=kind,
        question="보낼까?",
        options=["승인", "거절"],
        recommended=None,
        default_after=timedelta(hours=1),
        irreversible=False,
    )
    d.update(kw)  # type: ignore[typeddict-item]
    return d


def test_harden_approval_draft_reports_forced_fields_in_order():
    hardened, forced = harden_approval_draft(draft(forced=["recommended"]))
    assert forced == ["recommended", "default_after", "irreversible"]
    assert "forced" not in hardened
    assert hardened["irreversible"] is True and hardened["default_after"] is None
    hardened, forced = harden_approval_draft(draft(irreversible=True, default_after=None))
    assert forced == [] and hardened == draft(irreversible=True, default_after=None)
    # other kinds keep their fields; only what the MCP tool already forced is reported
    hardened, forced = harden_approval_draft(draft("decision", forced=["x"]))
    assert forced == ["x"] and hardened == draft("decision")
