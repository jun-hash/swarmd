"""Workers (orchestration layer): runs a WorkOrder end to end, from lease to verified result.

Facade: `WorkerPool.run` (`pool`); `contract` is the part callers may rely on. May import
shared, store, the domain packages, the seams `harness.base` and `engine.base`, and the `ports`
Guard and BrowserRuntime. It must never know a concrete adapter (no `browser`, `engine.claude`,
`harness.own`, registries) and never imports `coordinator`: the Coordinator sees `contract` only.
"""
