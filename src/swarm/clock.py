"""Time abstraction. All datetimes are tz-aware, in the configured time zone."""

from dataclasses import dataclass
from datetime import datetime, time, timedelta, tzinfo
from typing import Protocol
from zoneinfo import ZoneInfo

SEOUL = ZoneInfo("Asia/Seoul")  # the default of `config.timezone`


class Clock(Protocol):
    def now(self) -> datetime: ...


class SystemClock:
    def __init__(self, tz: tzinfo = SEOUL) -> None:
        self._tz = tz

    def now(self) -> datetime:
        return datetime.now(self._tz)


def zone_of(clock: Clock) -> tzinfo:
    """The clock's time zone: `config.timezone` for the system clock."""
    tz = clock.now().tzinfo
    if tz is None:
        raise ValueError("clock returned a naive datetime")
    return tz


class FakeClock:
    def __init__(self, start: datetime) -> None:
        if start.tzinfo is None:
            raise ValueError("FakeClock start must be tz-aware")
        self._now = start

    def now(self) -> datetime:
        return self._now

    def advance(self, td: timedelta) -> None:
        self._now += td


@dataclass(frozen=True)
class AlertWindow:
    """Local hours (in `tz`) in which non-urgent messages may go out: [start, end)."""

    start: time = time(8, 0)
    end: time = time(23, 0)
    tz: tzinfo = SEOUL

    def is_open(self, at: datetime) -> bool:
        t = at.astimezone(self.tz).time().replace(tzinfo=None)
        if self.start <= self.end:
            return self.start <= t < self.end
        return t >= self.start or t < self.end  # wraps midnight

    def next_open(self, at: datetime) -> datetime:
        local = at.astimezone(self.tz)
        opens = datetime.combine(local.date(), self.start, self.tz)
        return opens if opens > local else opens + timedelta(days=1)
