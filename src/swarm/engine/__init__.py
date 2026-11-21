"""Engines (adapters layer): the official CLIs that supply the model, `claude -p` today.

Facade: `Engine` in `engine.base`; `registry.build_engine` is the only way to get a concrete one.
May import shared and store. It must never know about browsers, harnesses or the run lifecycle,
so it never imports `browser` or `harness`. Consumers outside the daemon see `engine.base` only.
"""
