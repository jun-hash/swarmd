"""Shared primitives: interruptible sleep, tracked eager tasks, retry ladders, redaction, text."""

import asyncio
import logging

from swarm.aio import TaskSet, pause
from swarm.backoff import Ladder, doubling
from swarm.redact import REQUEST_LOGGERS, add_secret, redact
from swarm.text import clip, error_text

# --- backoff ----------------------------------------------------------------------------


def test_ladder_after_gives_up_past_the_last_step():
    assert [Ladder((60, 300, 900, 3600)).after(n) for n in range(1, 6)] == [
        60,
        300,
        900,
        3600,
        None,
    ]


def test_ladder_capped_repeats_the_last_step():
    assert [Ladder((300, 900, 3600)).capped(n) for n in (1, 3, 7)] == [300, 3600, 3600]


def test_doubling_is_capped():
    assert [doubling(n, 60, 1800) for n in (1, 2, 5, 6)] == [60, 120, 960, 1800]


def test_doubling_matches_a_loop_that_doubles_its_delay():
    """Each restart doubles the delay up to the cap, and a huge restart count never overflows."""
    lo, hi, delay = 0.01, 0.05, 0.01
    for restarts in range(1, 12):
        assert doubling(restarts, lo, hi) == delay
        delay = min(delay * 2, hi)
    assert doubling(5_000, 1.0, 60.0) == 60.0


# --- pause ------------------------------------------------------------------------------


async def test_pause_true_when_stopped():
    stop = asyncio.Event()
    asyncio.get_running_loop().call_later(0.01, stop.set)
    assert await asyncio.wait_for(pause(30, stop), 1) is True


async def test_pause_false_on_timeout():
    assert await pause(0.01, asyncio.Event()) is False


async def test_pause_returns_early_on_wake():
    stop, wake = asyncio.Event(), asyncio.Event()
    asyncio.get_running_loop().call_later(0.01, wake.set)
    assert await asyncio.wait_for(pause(30, stop, wake), 1) is False


async def test_pause_edges_stop_already_set_negative_seconds_wake_kept():
    stop, wake = asyncio.Event(), asyncio.Event()
    assert await asyncio.wait_for(pause(-5, stop), 1) is False  # negative: no sleep
    wake.set()
    assert await asyncio.wait_for(pause(30, stop, wake), 1) is False
    assert wake.is_set()  # the caller clears its own wake
    stop.set()
    assert await asyncio.wait_for(pause(30, stop), 1) is True


# --- TaskSet ----------------------------------------------------------------------------


async def test_taskset_task_cancelled_before_first_step_runs_its_finally():
    cleaned: list[str] = []

    async def body() -> None:
        try:
            await asyncio.sleep(30)
        finally:
            cleaned.append("finally")

    ts = TaskSet()
    task = ts.spawn(body(), "slow")
    task.cancel()
    await ts.drain()
    assert cleaned == ["finally"] and len(ts) == 0


async def test_taskset_on_done_sees_the_task_after_it_is_discarded():
    ts: TaskSet
    seen: list[tuple[str, int]] = []

    def on_done(task: asyncio.Task) -> None:
        seen.append((task.get_name(), len(ts)))

    async def instant() -> None:
        return None

    async def later() -> None:
        await asyncio.sleep(0)

    ts = TaskSet(on_done)
    ts.spawn(instant(), "instant")  # done at creation: on_done at once, never tracked
    assert seen == [("instant", 0)] and len(ts) == 0
    ts.spawn(later(), "later")
    assert len(ts) == 1
    await ts.drain()
    assert seen == [("instant", 0), ("later", 0)]


# --- redact -----------------------------------------------------------------------------


def test_redact_replaces_every_secret():
    add_secret("s3cr3t-a")
    add_secret("s3cr3t-b")
    assert redact("x s3cr3t-a y s3cr3t-b") == "x <token> y <token>"


def test_request_loggers_are_redacted(caplog):
    add_secret("s3cr3t-c")
    with caplog.at_level(logging.DEBUG):
        for name in REQUEST_LOGGERS:
            logging.getLogger(name).debug("GET /bot%s/x", "s3cr3t-c")
    assert len(caplog.records) == len(REQUEST_LOGGERS)
    assert "s3cr3t-c" not in caplog.text and caplog.text.count("<token>") == len(REQUEST_LOGGERS)


# --- text -------------------------------------------------------------------------------


def test_clip_and_error_text():
    assert clip("abcdef", 4) == "abc…" and clip("abc", 4) == "abc"
    assert error_text(ValueError("boom"), 9) == "ValueErro"
