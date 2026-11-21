"""Coordinator (orchestration layer): the agenda, conversation and reports. It never does
browser work itself.

Facade: `Coordinator.submit` (events in) and the agenda loop (`choose_next` picks what runs).
May import shared, store, the domain packages, `workers.contract` and `brain`.
It must never know how work is executed: no `workers.pool`, `browser`, `engine` or `harness`.
It reaches the pool through the `RunControl` Protocol and humans through `ports.Notifier`.
"""
