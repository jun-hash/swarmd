"""Resume / retry ladder / steer / takeover-release / verifier wiring / approval grant."""

import asyncio
import json
from datetime import datetime, timedelta

import pytest

from swarm.clock import SEOUL
from swarm.harness.base import AskRaised, Done, Error, FileOutput, QuotaSignal
from swarm.types import AskDraft, AskOutcome, Evidence
from swarm.workers.contract import IllegalTransition, LeaseExpired, RunFinished
from swarm.workers.verify import Verdict, VerifierUnavailable

from .conftest import BLOCK, V_OWN, order


def evidence(fakes) -> Evidence:
    return Evidence(
        url="http://c.test/price", screenshot=None, captured_at=fakes.clock.now(), text="₩9,900"
    )


def add_evidence(fakes, pool):
    def _hook(sink):
        pool.add_evidence(sink.run_id, evidence(fakes))

    return _hook


def raise_ask(kind="decision"):
    async def _hook(sink):
        await sink.raise_ask(
            AskDraft(
                kind=kind,
                question="보낼까?",
                options=["승인", "거절"],
                recommended=None,
                default_after=None,
                irreversible=kind == "approval",
                evidence=[],
            )
        )

    return _hook


ASK = [AskRaised(question="A or B?", irreversible=False), Done(summary="asked")]


def outcome(run_id, choice="B", status="answered", ask_id="ask_1") -> AskOutcome:
    return AskOutcome(
        ask_id=ask_id, run_id=run_id, status=status, choice=choice, raw_text=choice, via="t"
    )


async def settle():
    for _ in range(5):
        await asyncio.sleep(0)


async def park(fakes, pool, script=None, **kw):
    fakes.harness.scripts.append(script or list(ASK))
    r = await pool.run(order(**kw), V_OWN, lease=fakes.lease())
    await settle()
    assert r["status"] == "waiting_human"
    return r["run_id"]


def done_script(fakes, pool):
    return [add_evidence(fakes, pool), Done(summary="가격 ₩9,900")]


# --- resume ----------------------------------------------------------------------------


async def test_resume_with_session_gets_resume_id_and_message(fakes, pool):
    rid = await park(fakes, pool)
    fakes.harness.scripts.append(done_script(fakes, pool))
    r = await pool.resume(rid, outcome=outcome(rid, "B"), lease=fakes.lease())
    assert r["status"] == "done"
    res = fakes.harness.tasks[1]["resume"]
    assert res["session_id"] == "sess-9" and res["notes"] == ["n"]
    assert res["open_tabs"] == ["http://a/"] and res["message"] == "사람 답: B"
    assert fakes.browser.opened[0]["id"] == "bs_1" and len(fakes.browser.opened) == 1
    assert fakes.ledger.released == [fakes.lease(), fakes.lease()]
    kinds = fakes.kinds(rid)
    assert kinds.index("run.waiting_human") < kinds.index("run.running", 2)
    fakes.sleeper.open()  # the park timer was cancelled: nothing else closes
    await settle()
    assert fakes.browser.closed == ["bs_1"]


async def test_defaulted_outcome_message(fakes, pool):
    rid = await park(fakes, pool)
    fakes.harness.scripts.append(done_script(fakes, pool))
    await pool.resume(rid, outcome=outcome(rid, "A", status="defaulted"), lease=fakes.lease())
    assert fakes.harness.tasks[1]["resume"]["message"] == "기본값으로 진행: A"


async def test_pending_steers_prepended(fakes, pool):
    fakes.harness.pending_steers = ["표만 봐"]
    rid = await park(fakes, pool)
    assert await pool.steer(rid, text="B사도 봐") == "at_tool_boundary"
    fakes.harness.pending_steers = []
    fakes.harness.scripts.append(done_script(fakes, pool))
    await pool.resume(rid, outcome=outcome(rid, "B"), lease=fakes.lease())
    msg = fakes.harness.tasks[1]["resume"]["message"]
    assert msg == "사람 지시: 표만 봐\n사람 지시: B사도 봐\n사람 답: B"


async def test_resume_reopens_closed_browser(fakes, pool):
    rid = await park(fakes, pool)
    fakes.sleeper.open()  # 2 h later: session closed
    await settle()
    assert pool.session(rid) is None and fakes.browser.closed == ["bs_1"]
    fakes.trace.clear()
    fakes.harness.scripts.append(done_script(fakes, pool))
    r = await pool.resume(rid, outcome=outcome(rid, "B"), lease=fakes.lease())
    assert r["status"] == "done"
    assert [s["id"] for s in fakes.browser.opened] == ["bs_1", "bs_2"]
    assert fakes.trace.index("guard.arm:guarded") < fakes.trace.index("harness.start")
    msg = fakes.harness.tasks[1]["resume"]["message"]
    assert msg == "브라우저가 다시 열림, 마지막 URL 목록: http://a/\n사람 답: B"


async def test_resume_run_parked_before_reboot(fakes, pool):
    due = datetime(2026, 11, 7, 18, 0, tzinfo=SEOUL)
    rid = await park(fakes, pool, due=due)
    fresh = fakes.pool()  # new swarmd instance: no _live record, old session gone
    assert fresh.recover_on_boot() == []
    fakes.harness.scripts.append(done_script(fakes, fresh))
    r = await fresh.resume(rid, outcome=outcome(rid, "B"), lease=fakes.lease())
    assert r["status"] == "done"
    assert len(fakes.browser.opened) == 2  # a fresh session for the rebuilt run
    res = fakes.harness.tasks[1]["resume"]
    assert res["session_id"] == "sess-9" and res["message"].startswith("브라우저가 다시 열림")
    assert fakes.verifier.calls[-1][0]["due"] == due


async def test_expired_outcome_not_resumed(fakes, pool):
    rid = await park(fakes, pool)
    r = await pool.resume(rid, outcome=outcome(rid, None, status="expired"), lease=fakes.lease())
    assert r["status"] == "waiting_human" and len(fakes.harness.tasks) == 1
    assert pool.get(rid)["status"] == "waiting_human"


async def test_resume_finished_run_returns_its_result(fakes, pool):
    fakes.harness.scripts.append(done_script(fakes, pool))
    first = await pool.run(order(), V_OWN, lease=fakes.lease())
    r = await pool.resume(first["run_id"], outcome=outcome(first["run_id"]), lease=None)
    assert r["status"] == "done" and len(fakes.harness.tasks) == 1


async def test_resume_needs_a_live_lease(fakes, pool):
    rid = await park(fakes, pool)
    with pytest.raises(LeaseExpired):
        await pool.resume(rid, outcome=outcome(rid), lease=None)
    assert pool.get(rid)["status"] == "waiting_human"


async def test_quota_error_after_ask_then_answer_resumes(fakes, pool):
    sig = {"kind": "limit_hit", "reset_at": None, "at": fakes.clock.now()}
    fakes.harness.scripts.append(
        [AskRaised(question="A?", irreversible=False), QuotaSignal(signal=sig), Error("quota:x")]
    )
    r = await pool.run(order(), V_OWN, lease=fakes.lease())
    assert r["status"] == "interrupted" and fakes.inbox.expired == []
    fakes.harness.scripts.append(done_script(fakes, pool))
    r2 = await pool.resume(r["run_id"], outcome=outcome(r["run_id"], "A"), lease=fakes.lease())
    assert r2["status"] == "done"
    assert fakes.harness.tasks[1]["resume"]["message"].endswith("사람 답: A")


async def test_message_kept_until_delivered(fakes, pool):
    rid = await park(fakes, pool)
    fakes.sleeper.open()
    await settle()
    fakes.browser.fail_open = True
    r = await pool.resume(rid, outcome=outcome(rid, "B"), lease=fakes.lease())
    assert r["status"] == "interrupted"
    fakes.browser.fail_open = False
    fakes.harness.scripts.append(done_script(fakes, pool))
    r = await pool.resume(rid, outcome=outcome(rid, "C", ask_id="ask_2"), lease=fakes.lease())
    assert r["status"] == "done"
    msg = fakes.harness.tasks[1]["resume"]["message"]
    assert "사람 답: B" in msg and msg.endswith("사람 답: C")


# --- retry ladder ------------------------------------------------------------------------


async def test_engine_crash_resume_then_fresh_then_fail(fakes, pool):
    fakes.sleeper.open()
    fakes.harness.scripts += [[Error(message="boom")] for _ in range(3)]
    r = await pool.run(order(), V_OWN, lease=fakes.lease())
    assert r["status"] == "failed" and r["failure"]["why"] == "boom"
    t = fakes.harness.tasks
    assert len(t) == 3 and "resume" not in t[0]
    assert t[1]["resume"]["session_id"] == "sess-9"
    assert t[2]["resume"]["session_id"] is None and t[2]["resume"]["notes"] == ["n"]
    assert fakes.sleeper.calls == [60]
    assert fakes.projects.get("prj_compet")["memory"]["failed_attempts"][-1]["why"] == "boom"
    assert fakes.ledger.released == [fakes.lease()]


async def test_engine_crash_then_retry_succeeds(fakes, pool):
    fakes.harness.scripts += [[Error(message="boom")], done_script(fakes, pool)]
    r = await pool.run(order(), V_OWN, lease=fakes.lease())
    assert r["status"] == "done" and fakes.sleeper.calls == []
    assert "엔진 오류" in fakes.harness.tasks[1]["resume"]["message"]


# --- verifier hand-off -------------------------------------------------------------------


async def test_verify_pass_marks_artifacts_verified(fakes, pool):
    fakes.harness.scripts.append(done_script(fakes, pool))
    r = await pool.run(order(), V_OWN, lease=fakes.lease())
    assert r["status"] == "done"
    for a in r["artifacts"]:
        assert a["status"] == "verified" and a["verifier_run_id"] == "vrf_fake"
        assert a["verifier_same_engine"] is True and a["evidence"]
    (o, arts, evs) = fakes.verifier.calls[0]
    assert o["id"] == "ord_1" and evs and arts[0]["status"] == "reported"


async def test_verify_reject_retries_once_then_fails(fakes, pool):
    no = Verdict(pass_=False, reasons=["가격표 없음"], same_engine=True)
    fakes.verifier.verdicts = [no, no]
    fakes.harness.scripts += [done_script(fakes, pool), done_script(fakes, pool)]
    r = await pool.run(order(), V_OWN, lease=fakes.lease())
    assert r["status"] == "failed" and "가격표 없음" in r["failure"]["why"]
    assert fakes.harness.tasks[1]["resume"]["message"] == "검증 거절: 가격표 없음"
    assert len(fakes.harness.tasks) == 2
    assert all(a["status"] == "partial" for a in r["artifacts"])
    fails = fakes.projects.get("prj_compet")["memory"]["failed_attempts"]
    assert fails[-1]["run_id"] == r["run_id"]
    kinds = fakes.kinds(r["run_id"])
    assert kinds.count("run.verifying") == 2


async def test_verify_reject_then_pass(fakes, pool):
    fakes.verifier.verdicts = [Verdict(pass_=False, reasons=["x"], same_engine=True)]
    fakes.harness.scripts += [done_script(fakes, pool), done_script(fakes, pool)]
    r = await pool.run(order(), V_OWN, lease=fakes.lease())
    assert r["status"] == "done"


async def test_verifier_unavailable_interrupts(fakes, pool):
    async def boom(order, artifacts, evidence):
        raise VerifierUnavailable("cli down")

    fakes.verifier.verify = boom
    fakes.harness.scripts.append(done_script(fakes, pool))
    r = await pool.run(order(), V_OWN, lease=fakes.lease())
    assert r["status"] == "interrupted" and r["failure"] is None
    assert fakes.browser.closed == ["bs_1"]


# --- steer -------------------------------------------------------------------------------


async def test_steer_running_goes_to_adapter(fakes, pool):
    fakes.harness.scripts.append([BLOCK])
    task = asyncio.create_task(pool.run(order(), V_OWN, lease=fakes.lease()))
    await fakes.harness.started.wait()
    rid = pool.running()[0]["id"]
    assert await pool.steer(rid, text="B사도") == "at_tool_boundary"
    assert fakes.harness.steers == [(rid, "B사도")]
    assert pool.get(rid)["human_intervened"] is True
    ev = [e for e in fakes.log.query(run_id=rid) if e["kind"] == "human.steer"]
    assert ev and ev[0]["payload"]["text"] == "B사도"
    await pool.cancel(rid, reason="t")
    await task


async def test_steer_waiting_human_is_pending(fakes, pool):
    rid = await park(fakes, pool)
    assert await pool.steer(rid, text="천천히") == "at_tool_boundary"
    assert fakes.harness.steers == []
    assert pool.get(rid)["human_intervened"] is True
    assert "human.steer" in fakes.kinds(rid)


async def test_steer_finished_run_raises(fakes, pool):
    fakes.harness.scripts.append(done_script(fakes, pool))
    r = await pool.run(order(), V_OWN, lease=fakes.lease())
    with pytest.raises(RunFinished):
        await pool.steer(r["run_id"], text="x")


# --- takeover / release ------------------------------------------------------------------


async def test_takeover_running_disables_guard_release_reenables(fakes, pool):
    fakes.harness.scripts.append([BLOCK])
    task = asyncio.create_task(pool.run(order(), V_OWN, lease=fakes.lease()))
    await fakes.harness.started.wait()
    rid = pool.running()[0]["id"]
    info = await pool.takeover(rid)
    assert info["run_id"] == rid and info["profile"] == "work" and info["target_id"] == "T1"
    assert info["cdp_url"] == "ws://x" and info["instructions"]
    r = await task
    assert r["status"] == "waiting_human" and fakes.harness.closed
    assert pool.session(rid)["guard"] == "off"
    assert fakes.ledger.released == [fakes.lease()]
    assert pool.get(rid)["human_intervened"] is True and "human.takeover" in fakes.kinds(rid)
    fakes.sleeper.open()  # no 2 h close while a human holds the browser
    await settle()
    assert fakes.browser.closed == []

    fakes.ledger.next_lease = fakes.lease()
    fakes.harness.scripts.append(done_script(fakes, pool))
    await pool.release(rid, note="로그인 했음")
    r = await pool.wait(rid)
    assert r["status"] == "done"
    assert "guard.mode:guarded" in fakes.trace
    assert fakes.trace.index("guard.mode:guarded") < fakes.trace.index("harness.start", 3)
    assert fakes.harness.tasks[1]["resume"]["message"] == "사람이 이렇게 했음: 로그인 했음"
    assert fakes.ledger.leased[-1]["purpose"] == "advance"
    assert "human.release" in fakes.kinds(rid)
    assert fakes.db.execute("SELECT taken_over FROM runs WHERE id=?", (rid,)).fetchone()[0] == 0


async def test_takeover_parked_run_and_answer_waits_for_release(fakes, pool):
    rid = await park(fakes, pool)
    info = await pool.takeover(rid)
    assert info["profile"] == "work" and fakes.trace[-1] == "guard.mode:off"
    # an Ask answer while a human holds the browser waits for release
    r = await pool.resume(rid, outcome=outcome(rid, "B"), lease=fakes.lease())
    assert r["status"] == "waiting_human" and len(fakes.harness.tasks) == 1
    fakes.ledger.next_lease = fakes.lease()
    fakes.harness.scripts.append(done_script(fakes, pool))
    await pool.release(rid, note="됐어")
    assert (await pool.wait(rid))["status"] == "done"
    assert fakes.harness.tasks[1]["resume"]["message"] == "사람 답: B\n사람이 이렇게 했음: 됐어"


async def test_release_without_lease_keeps_takeover(fakes, pool):
    rid = await park(fakes, pool)
    await pool.takeover(rid)
    fakes.ledger.next_lease = None
    with pytest.raises(LeaseExpired):
        await pool.release(rid, note="됐어")
    assert pool.session(rid)["guard"] == "off"
    assert fakes.db.execute("SELECT taken_over FROM runs WHERE id=?", (rid,)).fetchone()[0] == 1


async def test_release_without_takeover_is_illegal(fakes, pool):
    rid = await park(fakes, pool)
    with pytest.raises(IllegalTransition):
        await pool.release(rid, note="x")


async def test_pending_persisted_as_json(fakes, pool):
    rid = await park(fakes, pool)
    await pool.steer(rid, text="s1")
    raw = fakes.db.execute("SELECT pending_json FROM runs WHERE id=?", (rid,)).fetchone()[0]
    assert json.loads(raw) == ["사람 지시: s1"]


def test_park_and_retry_constants():
    from swarm.workers import pool as p

    assert p.RETRY_FRESH_AFTER_S == 60 and p.PARK_SESSION_MAX == timedelta(hours=2)


# --- persisted result, verified invariant, file dedupe ------------------------------------


async def test_wait_and_resume_on_finished_run_return_persisted_result(fakes, pool):
    fakes.harness.scripts.append(done_script(fakes, pool))
    r = await pool.run(order(), V_OWN, lease=fakes.lease())
    for got in (
        await pool.wait(r["run_id"]),
        await pool.resume(r["run_id"], outcome=outcome(r["run_id"]), lease=None),
        await fakes.pool().wait(r["run_id"]),  # after a restart
    ):
        assert got["status"] == "done" and got["summary"] == r["summary"]
        assert [a["id"] for a in got["artifacts"]] == [a["id"] for a in r["artifacts"]]
        a = got["artifacts"][0]
        assert a["status"] == "verified" and a["evidence"][0]["captured_at"] == fakes.clock.now()


async def test_resume_on_finished_releases_callers_lease(fakes, pool):
    fakes.harness.scripts.append(done_script(fakes, pool))
    r = await pool.run(order(), V_OWN, lease=fakes.lease())
    fakes.ledger.released.clear()
    await pool.resume(r["run_id"], outcome=outcome(r["run_id"]), lease=fakes.lease())
    assert fakes.ledger.released == [fakes.lease()]


async def test_verified_needs_evidence_and_verifier_run_id(fakes, pool):
    ok_no_id = Verdict(pass_=True, reasons=[], same_engine=True, verifier_run_id=None)
    fakes.verifier.verdicts = [ok_no_id, ok_no_id]
    fakes.harness.scripts += [done_script(fakes, pool), done_script(fakes, pool)]
    r = await pool.run(order(), V_OWN, lease=fakes.lease())
    assert r["status"] == "failed" and all(a["status"] == "partial" for a in r["artifacts"])
    ok = Verdict(pass_=True, reasons=[], same_engine=True, verifier_run_id="vrf_x")
    fakes.verifier.verdicts = [ok, ok]
    fakes.harness.scripts += [[Done(summary="no evidence")], [Done(summary="still none")]]
    r = await pool.run(order(id="ord_2"), V_OWN, lease=fakes.lease())
    assert r["status"] == "failed"


async def test_file_outputs_deduped_across_turns(fakes, pool):
    shot = FileOutput(path="/tmp/s.png", kind="screenshot")
    fakes.harness.scripts += [[shot, Error(message="boom")], [shot, *done_script(fakes, pool)]]
    r = await pool.run(order(), V_OWN, lease=fakes.lease())
    assert r["status"] == "done"
    assert [a["path"] for a in r["artifacts"]][1:] == ["/tmp/s.png"]


# --- pending delivery, durable counters, queued answers -----------------------------------


def pending(fakes, rid) -> list[str]:
    raw = fakes.db.execute("SELECT pending_json FROM runs WHERE id=?", (rid,)).fetchone()[0]
    return json.loads(raw)


async def test_delivery_keeps_lines_appended_during_the_turn(fakes, pool):
    rid = await park(fakes, pool)
    fakes.harness.capabilities = frozenset({"resume"})  # no live steer: steers stay pending

    async def late_steer(sink):
        await pool.steer(rid, text="나중 지시")

    fakes.harness.scripts.append([late_steer, *ASK])
    r = await pool.resume(rid, outcome=outcome(rid, "B"), lease=fakes.lease())
    assert r["status"] == "waiting_human"
    assert fakes.harness.tasks[1]["resume"]["message"] == "사람 답: B"
    assert pending(fakes, rid) == ["사람 지시: 나중 지시"]


async def test_stale_checkpoint_steers_not_readded(fakes, pool):
    fakes.harness.pending_steers = ["표만 봐"]
    rid = await park(fakes, pool)
    assert pending(fakes, rid) == ["사람 지시: 표만 봐"]
    fakes.harness.checkpoints = False  # this turn leaves no checkpoint: the old one is stale
    fakes.harness.scripts.append(list(ASK))
    await pool.resume(rid, outcome=outcome(rid, "B"), lease=fakes.lease())
    assert fakes.harness.tasks[1]["resume"]["message"] == "사람 지시: 표만 봐\n사람 답: B"
    assert pending(fakes, rid) == []


async def test_second_answer_during_live_drive_is_queued(fakes, pool):
    rid = await park(fakes, pool)
    fakes.harness.started.clear()
    fakes.harness.scripts.append([BLOCK])
    first = asyncio.create_task(pool.resume(rid, outcome=outcome(rid, "B"), lease=fakes.lease()))
    await fakes.harness.started.wait()
    fakes.ledger.released.clear()
    second = asyncio.create_task(
        pool.resume(rid, outcome=outcome(rid, "C", ask_id="ask_2"), lease=fakes.lease())
    )
    await settle()
    assert "사람 답: C" in pending(fakes, rid) and not second.done()  # follows the live drive
    assert fakes.ledger.released == [fakes.lease()]  # its own lease went back at once
    assert "run.answer_queued" in fakes.kinds(rid)
    await pool.cancel(rid, reason="t")
    assert (await first)["status"] == (await second)["status"] == "cancelled"


async def test_retry_counters_survive_restart_and_reset_on_human_resume(fakes, pool):
    fakes.sleeper.open()
    fakes.harness.scripts += [[Error(message="boom")], list(ASK)]
    r = await pool.run(order(), V_OWN, lease=fakes.lease())
    rid = r["run_id"]
    assert r["status"] == "waiting_human"
    row = fakes.db.execute("SELECT retries, attempts FROM runs WHERE id=?", (rid,)).fetchone()
    assert tuple(row) == (1, 1)
    fresh = fakes.pool()  # restart
    fakes.harness.scripts += [[Error(message="boom")] for _ in range(3)]
    r = await fresh.resume(rid, outcome=outcome(rid, "B"), lease=fakes.lease())
    assert r["status"] == "failed"
    t = fakes.harness.tasks
    # human answer restarted the ladder: same session, then fresh, then failed
    assert t[3]["resume"]["session_id"] == "sess-9" and t[4]["resume"]["session_id"] is None
    attempts = [e["payload"]["attempt"] for e in fakes.log.query(run_id=rid, kinds=["run.retry"])]
    assert attempts == [1, 2, 3]


# --- takeover while a resume is still starting --------------------------------------------


@pytest.mark.parametrize("stage", ["open", "arm"])
async def test_takeover_during_resume_start_never_opens_second_session(fakes, pool, stage):
    rid = await park(fakes, pool)
    fakes.sleeper.open()  # session closed after 2 h: the resume must reopen it
    await settle()
    gate = asyncio.Event()
    if stage == "open":
        fakes.browser.open_gate = gate
        entered = fakes.browser.open_entered
    else:
        fakes.guard.arm_gate = gate
        entered = fakes.guard.arm_entered
    entered.clear()
    resuming = asyncio.create_task(pool.resume(rid, outcome=outcome(rid, "B"), lease=fakes.lease()))
    await entered.wait()
    assert pool.get(rid)["status"] == "waiting_human"  # driver live, not yet running
    take = asyncio.create_task(pool.takeover(rid))
    await settle()
    gate.set()
    info = await take
    r = await resuming
    assert r["status"] == "waiting_human"
    opened = [s["id"] for s in fakes.browser.opened]
    assert len(opened) == 2 and info["target_id"] == "T1"  # bs_1 (closed) + exactly one more
    assert pool.session(rid)["id"] == opened[-1] and pool.session(rid)["guard"] == "off"
    assert fakes.db.execute("SELECT taken_over FROM runs WHERE id=?", (rid,)).fetchone()[0] == 1
    assert len(fakes.harness.tasks) == 1  # the worker never ran
    assert "사람 답: B" in pending(fakes, rid)  # delivered on release


# --- dead browser session ---------------------------------------------------------------


def chrome_dies(fakes):
    def _hook(sink):
        fakes.browser.dead.update(s["id"] for s in fakes.browser.opened)

    return _hook


async def test_retry_after_a_chrome_crash_gets_a_fresh_session(fakes, pool):
    """Chrome dies mid-turn, every tool fails and the turn errors: the retry runs on a
    newly opened (and armed) session, told that the browser was reopened."""
    fakes.harness.scripts.append([chrome_dies(fakes), Error(message="tool failed: no target")])
    fakes.harness.scripts.append(done_script(fakes, pool))
    r = await pool.run(order(), V_OWN, lease=fakes.lease())
    assert r["status"] == "done"
    assert [s["id"] for s in fakes.browser.opened] == ["bs_1", "bs_2"]
    assert fakes.trace.count("guard.arm:guarded") == 2  # the fresh session was armed
    assert fakes.harness.seen_guard == ["guarded", "guarded"]
    assert fakes.harness.tasks[1]["resume"]["message"].startswith("브라우저가 다시 열림")
    assert "run.browser_lost" in fakes.kinds(r["run_id"])


async def test_resume_after_a_chrome_crash_while_parked_reopens(fakes, pool):
    rid = await park(fakes, pool)
    fakes.browser.dead.add("bs_1")  # Chrome crashed while the run waited for the human
    fakes.harness.scripts.append(done_script(fakes, pool))
    r = await pool.resume(rid, outcome=outcome(rid, "B"), lease=fakes.lease())
    assert r["status"] == "done"
    assert [s["id"] for s in fakes.browser.opened] == ["bs_1", "bs_2"]
    msg = fakes.harness.tasks[1]["resume"]["message"]
    assert msg == "브라우저가 다시 열림, 마지막 URL 목록: http://a/\n사람 답: B"


async def test_a_disconnected_guard_counts_as_a_dead_session(fakes, pool):
    rid = await park(fakes, pool)
    fakes.guard.connected = False  # the guard's CDP connection died with Chrome
    fresh = type(fakes.guard)(fakes.trace)
    fakes.guards = lambda b, p: _ret(fresh)  # the provider hands out a new guard
    pool._guards = fakes.guards
    fakes.harness.scripts.append(done_script(fakes, pool))
    r = await pool.resume(rid, outcome=outcome(rid, "B"), lease=fakes.lease())
    assert r["status"] == "done"
    assert len(fakes.browser.opened) == 2 and fresh.disarmed == ["bs_2"]  # armed, then closed


async def _ret(x):
    return x
