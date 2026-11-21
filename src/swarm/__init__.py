"""swarmd: an always-on browser employee, run as one asyncio daemon on one machine.

It takes goals from a human (Telegram via Hermes), keeps a backlog per project, does browser work
through an engine (the official `claude` CLI) driving Chrome over CDP, verifies the result with
evidence, and asks a human before any irreversible request. This docstring is the architecture
map; `tests/test_layers.py` enforces the layers below on the import statements.

Layers, highest first. A module may import its own layer and any layer below it::

     process        daemon · watchdog · cli · housekeeping
        |             wires everything; only daemon builds concretes
     front          front/*
        |             uses the coordinator facade, inbox, projects, ports;
        |             never workers, browser, engine, harness
     orchestration  coordinator/*  -> workers.contract (never workers.pool) · brain
        |           workers/*      -> ports.Guard / BrowserRuntime · harness.base · engine.base
        |                             (never a concrete adapter or a registry)
     domain         projects · inbox · scheduler
        |             know each other only through the Protocols in ports
     adapters       harness -> engine.base, browser.cdp  ·  engine  ·  browser
        |             never upward, never each other's internals
     store          store/*
        |
     shared         types · ids · clock · ports · config · aio · backoff · redact · text

Packages, each with one facade:

- `coordinator`: decides what to do next. `Coordinator.submit` and the agenda loop.
- `workers`: runs one WorkOrder from lease to verified result. `WorkerPool.run`.
- `harness`: how browser work is done. `HarnessAdapter.run` (`own` today).
- `engine`: the model CLI behind a harness. `Engine` and `build_engine`.
- `browser`: the CDP runtime and the fail-closed action guard. `BrowserRuntime.open_session`.
- `scheduler`: when an engine may be used. `QuotaLedger.lease`.
- `projects`: objectives, key results, backlog, and their files. `ProjectStore`.
- `inbox`: the Ask lifecycle, questions waiting for a human. `Inbox`.
- `store`: SQLite, event log, blobs. `Database`.
- `front`: humans and Hermes reach swarmd here. `SwarmFrontService` (MCP tools for Hermes);
  the Telegram notifier, the internal API for workers and the kanban mirror sit beside it.
  `front.kanban`: `KanbanMirror`.

The shared layer is the vocabulary: value types (`types`), the cross-layer Protocols (`ports`),
config, clock and small async helpers. `brain` is the engine-backed judgment the Coordinator
uses; `housekeeping` is the daemon's own login check and blob prune.
"""
