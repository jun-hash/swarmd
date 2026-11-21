"""EngineBrain: schema-validated single-shot LLM decisions behind a quota lease."""

import math
import time
from typing import Protocol, TypeVar

from pydantic import BaseModel, ValidationError

from swarm.clock import Clock, SystemClock
from swarm.engine.base import EngineError
from swarm.ports import BrainError, BrainPurpose
from swarm.store.events import Emitter, EventLog

T = TypeVar("T", bound=BaseModel)

MAX_ATTEMPTS = 2
SCHEMA_ERROR_MAX = 500  # a validation error fed back into the retry prompt


class SingleShotEngine(Protocol):
    id: str

    async def single_shot(self, *, prompt: str, json_schema: dict, timeout_s: int) -> dict: ...


class EngineBrain:
    def __init__(
        self,
        engine: SingleShotEngine,
        ledger,
        log: EventLog,
        clock: Clock | None = None,
    ) -> None:
        self._engine = engine
        self._ledger = ledger
        self._clock = clock or SystemClock()
        self._emitter = Emitter(log, self._clock, "brain")

    async def decide(
        self, *, purpose: BrainPurpose, prompt: str, schema: type[T], timeout_s: int = 60
    ) -> T:
        lease = self._ledger.lease(
            engine_pref=[self._engine.id],
            purpose="respond",
            max_minutes=max(1, math.ceil(timeout_s / 60)),
        )
        if lease is None:
            self._emit(purpose, 0, len(prompt), False, 0)
            raise BrainError("engine unavailable")
        t0 = time.monotonic()
        attempts = 0
        ok = False
        try:
            json_schema = schema.model_json_schema()
            current = prompt
            last_err = ""
            while attempts < MAX_ATTEMPTS:
                attempts += 1
                try:
                    raw = await self._engine.single_shot(
                        prompt=current, json_schema=json_schema, timeout_s=timeout_s
                    )
                except EngineError as e:
                    if e.signal is not None:
                        self._ledger.observe(engine=self._engine.id, signal=e.signal)
                    raise BrainError(f"engine error: {e}") from e
                try:
                    result = schema.model_validate(raw)
                except ValidationError as e:
                    last_err = str(e)[:SCHEMA_ERROR_MAX]
                    current = (
                        f"{prompt}\n\nYour previous output failed schema validation:\n"
                        f"{last_err}\nReturn JSON that matches the schema exactly."
                    )
                    continue
                ok = True
                return result
            raise BrainError(f"schema validation failed after {attempts} attempts: {last_err}")
        finally:
            self._ledger.release(lease)
            self._emit(purpose, int((time.monotonic() - t0) * 1000), len(prompt), ok, attempts)

    def _emit(self, purpose: str, ms: int, input_chars: int, ok: bool, attempts: int) -> None:
        self._emitter(
            "brain.call",
            {
                "purpose": purpose,
                "engine": self._engine.id,
                "ms": ms,
                "input_chars": input_chars,
                "ok": ok,
                "attempts": attempts,
            },
        )
