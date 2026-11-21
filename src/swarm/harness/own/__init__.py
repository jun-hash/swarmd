"""The own harness: an engine drives the browser through the `swarm-browser-mcp` tools.

Facade: `adapter.OwnHarness`, a `HarnessAdapter`; `context` picks the rollover policy, `prompts`
holds the instructions, `observe` reads pages, `browser_mcp` is the tool server process.
Follows the harness package rules: `engine.base` and `browser.cdp` only. It must never know
how a request is guarded or approved: that is the worker pool and the internal HTTP API.
"""
