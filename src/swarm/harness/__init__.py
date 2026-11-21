"""Harnesses (adapters layer): how browser work gets done, `own` today.

Facade: `HarnessAdapter.run` in `harness.base`; `registry.HarnessRegistry` builds adapters.
May import shared, store, `engine.base` and `browser.cdp`, nothing else of engine or browser.
It must never know the concrete engine or runtime: not `engine.claude`, `browser.chrome` or
`browser.guard`. The worker pool sees `harness.base` only, never an adapter or the registry.
"""
