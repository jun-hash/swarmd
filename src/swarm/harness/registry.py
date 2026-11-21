"""Harness registry: `registry.get(variant.harness)` -> HarnessAdapter.

Adapters are built from shared deps by a factory per HarnessId. The registry caches instances,
because `steer(handle, ...)` must reach the same instance that is running the handle's run.
The contract test iterates ADAPTER_FACTORIES, so a new adapter is covered by registering it here.
"""

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from swarm.clock import Clock
from swarm.harness.base import HarnessAdapter
from swarm.harness.own.adapter import OwnHarness
from swarm.harness.own.context import ContextPolicy, get_policy
from swarm.types import BrowserId, EngineId, HarnessId


@dataclass(frozen=True)
class HarnessDeps:
    mcp_entry: str  # command for the browser MCP (swarm-browser-mcp)
    internal_url: str  # daemon internal API base URL
    token_file: Path  # bearer token for the internal API
    context_policy: ContextPolicy  # default policy for own
    blob_dir: Path | None = None  # screenshots; None -> each run's sink.output_dir
    clock: Clock | None = None


def _own(deps: HarnessDeps, policy: ContextPolicy | None) -> HarnessAdapter:
    return OwnHarness(
        policy or deps.context_policy,
        deps.mcp_entry,
        deps.internal_url,
        deps.token_file,
        blob_dir=deps.blob_dir,
        clock=deps.clock,
    )


Factory = Callable[[HarnessDeps, ContextPolicy | None], HarnessAdapter]
ADAPTER_FACTORIES: dict[HarnessId, Factory] = {"own": _own}

# (harness, engine, browser) combinations a run may use; adding one is a line here
COMPATIBLE: frozenset[tuple[HarnessId, EngineId, BrowserId]] = frozenset(
    {("own", "claude", "chrome")}
)


class HarnessRegistry:
    def __init__(self, deps: HarnessDeps) -> None:
        self._deps = deps
        self._cache: dict[tuple[HarnessId, str | None], HarnessAdapter] = {}

    def ids(self) -> list[HarnessId]:
        return sorted(ADAPTER_FACTORIES)

    def get(self, harness_id: HarnessId, *, context_policy: str | None = None) -> HarnessAdapter:
        """Adapter for `harness_id` (KeyError if unknown). `context_policy` (Variant field)
        selects a ContextPolicy by id for adapters that use one; others ignore it."""
        key = (harness_id, context_policy)
        if key not in self._cache:
            factory = ADAPTER_FACTORIES[harness_id]
            policy = get_policy(context_policy) if context_policy else None
            self._cache[key] = factory(self._deps, policy)
        return self._cache[key]
