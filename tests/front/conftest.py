from datetime import datetime

import pytest
from fakes import FakeTelegram

from swarm.clock import SEOUL, FakeClock
from swarm.types import Outbound

TOKEN = "123456:SECRET-bot-token"
CH = "telegram:-100123:42"
DAY = datetime(2026, 11, 5, 10, 0, tzinfo=SEOUL)  # inside the 08:00-23:00 window


def out(**kw) -> Outbound:
    base = dict(channel=CH, kind="fyi", text="ℹ️ FYI · 안녕", ref_id=None, urgent=False)
    base.update(kw)
    return Outbound(**base)  # type: ignore[typeddict-item]


class InboxSpy:
    """The two Inbox calls the notifier makes. Register open Asks in `open_ids`."""

    def __init__(self) -> None:
        self.marked: list[tuple[str, datetime]] = []
        self.open_ids: set[str] = set()

    def mark_sent(self, ask_id, at):
        self.marked.append((ask_id, at))

    def open_asks(self, project_id=None):
        return [{"ask_id": a} for a in sorted(self.open_ids)]


# Overrides the root clock: notifier tests need a day inside the 08:00-23:00 window.
@pytest.fixture
def clock():
    return FakeClock(DAY)


@pytest.fixture
def fake_tg():
    return FakeTelegram()


@pytest.fixture
def inbox_spy():
    return InboxSpy()
