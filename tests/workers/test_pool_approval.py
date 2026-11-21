"""Approval gate: the only path to an irreversible request. A false grant is never OK;
a missed one only makes the worker ask again."""

from datetime import timedelta

import pytest

from swarm.harness.base import AskRaised, Done
from swarm.types import AskDraft, AskOutcome, Event, Evidence

from .conftest import V_OWN, order

DESC = {
    "method": "POST",
    "host": "shop.test",
    "path": "/order?id=1",
    "body_sha256": "x",
    "fingerprint": "f",
}
BEACON = {**DESC, "path": "/beacon", "fingerprint": "b"}
ASK = [AskRaised(question="보낼까?", irreversible=True), Done(summary="asked")]


def block(fakes, d=DESC, *, reason="needs_token", order_id="ord_1", run_id=None):
    def _hook(sink):
        fakes.log.append(
            Event(
                id="",
                ts=fakes.clock.now(),
                kind="guard.blocked",
                actor="guard",
                project_id=None,
                run_id=run_id or sink.run_id,
                payload={
                    "order_id": order_id,
                    "mode": "guarded",
                    "reason": reason,
                    "descriptor": d,
                },
                refs=[],
            )
        )

    return _hook


def ask(kind="approval", options=("승인", "거절"), default_after=None, irreversible=True):
    async def _hook(sink):
        await sink.raise_ask(
            AskDraft(
                kind=kind,
                question="보낼까?",
                options=list(options),
                recommended=None,
                default_after=default_after,
                irreversible=irreversible,
                evidence=[],
            )
        )

    return _hook


def outcome(rid, choice, *, status="answered", via="telegram:-100:1", raw=None, ask_id="ask_1"):
    return AskOutcome(
        ask_id=ask_id, run_id=rid, status=status, choice=choice, raw_text=raw, via=via
    )


def done(fakes, pool):
    def _ev(sink):
        pool.add_evidence(
            sink.run_id,
            Evidence(
                url="http://c.test/", screenshot=None, captured_at=fakes.clock.now(), text="t"
            ),
        )

    return [_ev, Done(summary="ok")]


async def park(fakes, pool, script, **kw):
    fakes.harness.scripts.append(script)
    r = await pool.run(order(**kw), V_OWN, lease=fakes.lease())
    assert r["status"] == "waiting_human"
    return r["run_id"]


async def resume(fakes, pool, rid, out):
    fakes.clock.advance(timedelta(minutes=1))
    fakes.harness.scripts.append(done(fakes, pool))
    return await pool.resume(rid, outcome=out, lease=fakes.lease())


async def test_grant_binds_the_request_blocked_before_the_ask(fakes, pool):
    rid = await park(fakes, pool, [block(fakes), ask(), block(fakes, BEACON), *ASK])
    fakes.trace.clear()
    r = await resume(fakes, pool, rid, outcome(rid, "승인"))
    assert r["status"] == "done"
    (tok,) = fakes.guard.granted
    assert tok.ask_id == "ask_1" and tok.order_id == "ord_1"
    assert (tok.method, tok.host, tok.path_pattern) == ("POST", "shop.test", "/order[?]id=1")
    assert fakes.trace.index("guard.grant") < fakes.trace.index("harness.start")


@pytest.mark.parametrize("choice", ["Approve", "YES", "허용"])
async def test_exact_approving_option_case_insensitive(fakes, pool, choice):
    rid = await park(fakes, pool, [block(fakes), ask(options=[choice, "no"]), *ASK])
    await resume(fakes, pool, rid, outcome(rid, choice))
    assert len(fakes.guard.granted) == 1


@pytest.mark.parametrize(
    "options,choice",
    [
        (["보내기", "보내지 않기"], "보내지 않기"),
        (["승인", "거절"], "거절"),
        (["approve", "do not approve"], "do not approve"),
        (["승인", "거절"], "승인해"),  # not one of the options
        (["승인 후 보내기", "거절"], "승인 후 보내기"),  # option is not an approving word
    ],
)
async def test_refusals_and_non_options_never_grant(fakes, pool, options, choice):
    rid = await park(fakes, pool, [block(fakes), ask(options=options), *ASK])
    await resume(fakes, pool, rid, outcome(rid, choice))
    assert fakes.guard.granted == []


async def test_raw_text_only_never_grants(fakes, pool):
    rid = await park(fakes, pool, [block(fakes), ask(), *ASK])
    await resume(fakes, pool, rid, outcome(rid, None, raw="승인"))
    assert fakes.guard.granted == []


@pytest.mark.parametrize(
    "status,via", [("defaulted", "default"), ("answered", "policy"), ("answered", "default")]
)
async def test_policy_or_default_never_grants(fakes, pool, status, via):
    rid = await park(fakes, pool, [block(fakes), ask(), *ASK])
    await resume(fakes, pool, rid, outcome(rid, "승인", status=status, via=via))
    assert fakes.guard.granted == []


async def test_decision_ask_never_grants(fakes, pool):
    rid = await park(fakes, pool, [block(fakes), ask(kind="decision"), *ASK])
    await resume(fakes, pool, rid, outcome(rid, "승인"))
    assert fakes.guard.granted == []


async def test_block_from_earlier_segment_or_run_not_used(fakes, pool):
    # another run of the same order blocked a request; this run never did
    rid = await park(fakes, pool, [block(fakes, run_id="run_other"), ask(), *ASK])
    await resume(fakes, pool, rid, outcome(rid, "승인"))
    assert fakes.guard.granted == []
    assert "run.approval_unused" in fakes.kinds(rid)
    # a block in the first turn, the approval Ask only in a later turn
    rid = await park(
        fakes, pool, [block(fakes, order_id="ord_2"), ask("decision"), *ASK], id="ord_2"
    )
    fakes.clock.advance(timedelta(minutes=1))
    fakes.harness.scripts.append([ask(), *ASK])
    r = await pool.resume(rid, outcome=outcome(rid, "A", ask_id="ask_2"), lease=fakes.lease())
    assert r["status"] == "waiting_human"
    await resume(fakes, pool, rid, outcome(rid, "승인", ask_id="ask_3"))
    assert fakes.guard.granted == []


async def test_other_block_reasons_not_bound(fakes, pool):
    rid = await park(fakes, pool, [block(fakes, reason="dry_run"), ask(), *ASK])
    await resume(fakes, pool, rid, outcome(rid, "승인"))
    assert fakes.guard.granted == []


async def test_approval_ask_forced_irreversible(fakes, pool):
    rid = await park(
        fakes,
        pool,
        [ask(default_after=timedelta(minutes=30), irreversible=False), *ASK],
    )
    (_, draft) = fakes.inbox.asks[0]
    assert draft["irreversible"] is True and draft["default_after"] is None
    assert "ask.irreversible_forced" in fakes.kinds(rid)


async def test_two_distinct_blocked_requests_bind_nothing(fakes, pool):
    rid = await park(fakes, pool, [block(fakes), block(fakes, BEACON), ask(), *ASK])
    await resume(fakes, pool, rid, outcome(rid, "승인"))
    assert fakes.guard.granted == []
    (ev,) = fakes.log.query(run_id=rid, kinds=["ask.approval_unbound"])
    assert {d["path"] for d in ev["payload"]["candidates"]} == {"/order?id=1", "/beacon"}


async def test_same_request_blocked_twice_still_binds(fakes, pool):
    rid = await park(fakes, pool, [block(fakes), block(fakes), ask(), *ASK])
    await resume(fakes, pool, rid, outcome(rid, "승인"))
    assert len(fakes.guard.granted) == 1


async def test_forced_flag_from_mcp_draft_is_logged_and_stripped(fakes, pool):
    async def mcp_style(sink):
        await sink.raise_ask(
            AskDraft(
                kind="approval",
                question="보낼까?",
                options=["승인", "거절"],
                recommended=None,
                default_after=None,
                irreversible=True,
                evidence=[],
                forced=["default_after", "options"],
            )
        )

    rid = await park(fakes, pool, [mcp_style, *ASK])
    (ev,) = fakes.log.query(run_id=rid, kinds=["ask.irreversible_forced"])
    assert ev["payload"]["forced"] == ["default_after", "options"]
    assert "forced" not in fakes.inbox.asks[0][1]
