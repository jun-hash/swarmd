"""Scripted engine for contract tests of later layers."""

from collections.abc import AsyncIterator

from swarm.engine.base import EngineError, EngineEvent, EngineTurn


class FakeRun:
    def __init__(self, script: list[EngineEvent], session_id: str | None) -> None:
        self._script = script
        self.session_id = session_id
        self.interrupted = False

    async def events(self) -> AsyncIterator[EngineEvent]:
        for ev in self._script:
            if self.interrupted:
                return
            yield ev

    async def interrupt(self) -> None:
        self.interrupted = True


class FakeEngine:
    id = "claude"

    def __init__(self, script: list[EngineEvent], session_id: str | None = "fake-session") -> None:
        self.script = list(script)
        self.session_id = session_id
        self.turns: list[EngineTurn] = []
        self.runs: list[FakeRun] = []
        self.single_shot_calls: list[dict] = []
        self._shots: list[dict] = []

    def single_shot_returns(self, *shots: dict) -> None:
        self._shots.extend(shots)

    async def start(self, turn: EngineTurn) -> FakeRun:
        self.turns.append(turn)
        run = FakeRun(list(self.script), self.session_id)
        self.runs.append(run)
        return run

    async def single_shot(self, *, prompt: str, json_schema: dict, timeout_s: int) -> dict:
        self.single_shot_calls.append(
            {"prompt": prompt, "json_schema": json_schema, "timeout_s": timeout_s}
        )
        if not self._shots:
            raise EngineError("FakeEngine: no single_shot result queued")
        return self._shots.pop(0)
