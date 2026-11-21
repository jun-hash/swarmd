"""Usage-limit injection for the soak test.

With `SWARM_SIMULATE_LIMIT=1` in swarmd's environment, the FIRST engine call after boot (a
worker turn or a Brain / Verifier single shot) does not reach the CLI: it reports a usage limit
exactly as the `claude` CLI does, a `result` line with `is_error` and a "usage limit reached"
text, run through the real ClaudeEngine parser. So the pool, Brain and QuotaLedger see the
signal they already handle (`limit_hit`, no reset time -> the ledger cools the engine for an
hour). Every later call goes to the real engine. Unset (or any other value): not installed.

`scripts/soak.py orchestrate` sets the variable for one restart of swarmd and unsets it again.
"""

import json
import os
from collections.abc import AsyncIterator, Mapping

from swarm.clock import Clock
from swarm.engine.base import Engine, EngineError, EngineEvent, EngineTurn, quota_error
from swarm.engine.claude import ParseState, classify_text, parse_line

ENV = "SWARM_SIMULATE_LIMIT"
SIMULATED_TEXT = "Claude AI usage limit reached (simulated: SWARM_SIMULATE_LIMIT=1)"


class _LimitRun:
    session_id: str | None = None

    def __init__(self, clock: Clock) -> None:
        self._clock = clock

    async def events(self) -> AsyncIterator[EngineEvent]:
        st = ParseState(clock=self._clock)
        line = json.dumps({"type": "result", "is_error": True, "result": SIMULATED_TEXT})
        for ev in parse_line(line, st):
            yield ev
        yield quota_error(st.signal_kind)  # what ClaudeRun ends a limited run with

    async def interrupt(self) -> None:
        return None


class LimitSimulatingEngine:
    """Wraps the real engine; only the first call is replaced by the limit signal."""

    def __init__(self, inner: Engine, clock: Clock) -> None:
        self._inner = inner
        self._clock = clock
        self.id = inner.id
        self._pending = True

    def _fire(self) -> bool:
        fire, self._pending = self._pending, False
        return fire

    async def start(self, turn: EngineTurn):
        if self._fire():
            return _LimitRun(self._clock)
        return await self._inner.start(turn)

    async def single_shot(self, *, prompt: str, json_schema: dict, timeout_s: int) -> dict:
        if self._fire():
            raise EngineError(SIMULATED_TEXT, classify_text(SIMULATED_TEXT, self._clock))
        return await self._inner.single_shot(
            prompt=prompt, json_schema=json_schema, timeout_s=timeout_s
        )


def wrap_limit_simulation(
    engine: Engine, clock: Clock, env: Mapping[str, str] | None = None
) -> Engine:
    """`engine` itself unless `SWARM_SIMULATE_LIMIT=1`."""
    if (os.environ if env is None else env).get(ENV) != "1":
        return engine
    return LimitSimulatingEngine(engine, clock)
