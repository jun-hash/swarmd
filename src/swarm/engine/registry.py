"""Engine registry: `build_engine(engine_id, ...)` -> Engine (the harness registry's twin).

An engine is one module implementing `Engine` plus one line in ENGINE_FACTORIES. To run work on
it, also add its (harness, engine, browser) rows to harness.registry.COMPATIBLE and a quota cap
in the ledger's `caps` (today the default in scheduler/ledger.py: the daemon passes none), and
name it in daemon.default_variant. The daemon's login check (`default_auth_check`) and the
engine settings in `Config` are still claude's.
"""

from collections.abc import Callable
from pathlib import Path

from swarm.clock import Clock
from swarm.config import Config
from swarm.engine.base import Engine
from swarm.engine.claude import ClaudeEngine
from swarm.types import EngineId

EngineFactory = Callable[[Config, Path, Clock], Engine]


def _claude(cfg: Config, workdir: Path, clock: Clock) -> Engine:
    return ClaudeEngine(cfg.claude_binary, cfg.claude_model, workdir, clock=clock)


ENGINE_FACTORIES: dict[EngineId, EngineFactory] = {"claude": _claude}


def build_engine(engine_id: EngineId, cfg: Config, workdir: Path, clock: Clock) -> Engine:
    """The engine registered under `engine_id` (KeyError if unknown)."""
    return ENGINE_FACTORIES[engine_id](cfg, workdir, clock)
