from datetime import datetime

import pytest

from swarm.clock import SEOUL, FakeClock
from swarm.store.events import Emitter


class RecordingLog:
    def __init__(self, fail: bool = False) -> None:
        self.events: list[dict] = []
        self.fail = fail

    def append(self, ev) -> str:
        if self.fail:
            raise RuntimeError("log full")
        self.events.append(ev)
        return "evt_1"


# Overrides the root clock with the 09:00 start of this package's fake_clock.
@pytest.fixture
def clock():
    return FakeClock(datetime(2026, 11, 6, 9, 0, tzinfo=SEOUL))


def test_emitter_builds_the_same_event_as_the_hand_built_one(clock):
    log = RecordingLog()
    emit = Emitter(log, clock, "coordinator")
    emit("agenda.tick", {"a": 1, "b": 2}, project_id="p1", run_id="r1", b=3, c=4)
    assert log.events == [
        {
            "id": "",
            "ts": clock.now(),
            "kind": "agenda.tick",
            "actor": "coordinator",
            "project_id": "p1",
            "run_id": "r1",
            "payload": {"a": 1, "b": 3, "c": 4},
            "refs": [],
        }
    ]
    assert list(log.events[0]["payload"]) == ["a", "b", "c"]  # payload first, then fields


def test_emitter_defaults_to_no_project_run_or_payload(clock):
    log = RecordingLog()
    Emitter(log, clock, "daemon")("daemon.started")
    ev = log.events[0]
    assert (ev["project_id"], ev["run_id"], ev["payload"]) == (None, None, {})


def test_emitter_without_a_log_is_a_noop(clock):
    Emitter(None, clock, "notifier")("x.y", {"a": 1})


def test_emitter_propagates_log_errors_by_default(clock):
    with pytest.raises(RuntimeError):
        Emitter(RecordingLog(fail=True), clock, "workers")("x.y")


def test_emitter_best_effort_swallows_log_errors(clock):
    Emitter(RecordingLog(fail=True), clock, "kanban", best_effort=True)("x.y")


def test_emitter_best_effort_swallows_a_failing_clock():
    class BrokenClock:
        def now(self):
            raise RuntimeError("no time")

    Emitter(RecordingLog(), BrokenClock(), "daemon", best_effort=True)("x.y")
