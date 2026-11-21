"""Retry schedules: a fixed ladder of waits, and a doubling wait with a cap."""

from dataclasses import dataclass


@dataclass(frozen=True)
class Ladder:
    """Waits (seconds) before each retry; `failures` counts from 1."""

    steps: tuple[float, ...]

    def after(self, failures: int) -> float | None:
        """The wait after this many failures, or None once the ladder is used up (give up)."""
        return self.steps[failures - 1] if failures <= len(self.steps) else None

    def capped(self, failures: int) -> float:
        """The wait after this many failures; past the last step, the last step again."""
        return self.steps[min(failures, len(self.steps)) - 1]


def doubling(failures: int, base_s: float, cap_s: float) -> float:
    """base, 2x base, 4x base ... capped at `cap_s`; `failures` counts from 1."""
    try:
        return min(base_s * 2 ** (failures - 1), cap_s)
    except OverflowError:  # a float base times a huge power of two: far past any cap
        return cap_s
