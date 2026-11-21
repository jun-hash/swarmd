# swarmd

swarmd is an always-on browser employee: one Python 3.12 asyncio daemon on a Mac. It takes goals
from a human over Telegram (Hermes is the front door), keeps a backlog per project, and does the
browser work by driving Chrome over CDP with an official engine CLI (`claude`). Every result is
verified against evidence, and any request that could change something outside the browser
waits for a human's approval.

Status: pre-release, built to be read and extended. See "Open items" for what is not settled.

## Architecture

The code is split into layers. A module may import its own layer and any layer below it.

```
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
```

Rules, all checked on import statements by `tests/test_layers.py`:

- Never import upward.
- Packages of one layer do not import each other, except the edges the test lists (for example
  `harness` may use `engine.base` and `browser.cdp`; `coordinator` may use `workers.contract`).
- Only `daemon` builds concrete adapters. `workers` sees the `harness.base` and `engine.base`
  seams, never a concrete adapter or a registry; `coordinator` never imports `workers.pool`.
- A module does not import another module's `_private` names.

Each package has one facade, and its `__init__.py` docstring says what it may import and what it
must never know. `src/swarm/__init__.py` holds the full map.

| Package | Facade |
|---|---|
| `coordinator` | `Coordinator.submit` and the agenda loop |
| `workers` | `WorkerPool.run` |
| `harness` | `HarnessAdapter.run` |
| `browser` | `BrowserRuntime.open_session` |
| `scheduler` | `QuotaLedger.lease` |
| `projects` | `ProjectStore` |
| `inbox` | `Inbox` |
| `front` | `SwarmFrontService` (MCP tools); `front.kanban`: `KanbanMirror` |

## Quickstart

Needs macOS, Chrome, the `claude` CLI logged in, a Telegram bot with a forum group, and
[uv](https://docs.astral.sh/uv/).

```sh
uv sync
uv run swarm init --telegram-token <bot-token> --general-channel telegram:<chat_id>:1 \
    --profile work=<chrome-user-data-dir>
uv run swarm doctor        # claude, Chrome, Hermes, Telegram, swarmd (swarmd fails until it runs)
uv run swarm project create --slug compet --name "Competitor watch" \
    --channel telegram:<chat_id>:<thread_id> --objective "Track competitor pricing" \
    --kr "kr1|pages checked|20|weekly"
uv run swarmd              # the daemon (one instance per home; a second one exits)
uv run swarm tell telegram:<chat_id>:<thread_id> "check the pricing page today"
```

`swarm init` writes `~/swarm/config.toml` (mode 0600) and a secret. Other commands are
`swarm status` (state summary) and `swarm watchdog` (one watchdog pass). `swarm` takes
`--config PATH` before the subcommand (`swarm --config PATH status`); `swarmd --config PATH`
takes it directly. The default is `~/swarm/config.toml`. `swarm project create` also takes
`--weight` and `--profile`.

Config keys: `home`, `timezone` (required; `swarm init` writes `"Asia/Seoul"`; the daemon's clock,
and so the quiet hours, runs in it), `alert_start` / `alert_end`, `telegram_bot_token`,
`general_channel`, `chrome_binary`, `hermes_dashboard_url`, `hermes_session_token`,
`internal_port`, `claude_binary`, `claude_model`, `guard_allowlist` and `[profiles]`. `swarm init`
writes a template with them.

Console scripts (`pyproject.toml`): `swarmd`, `swarm`, `swarm-browser-mcp` (the browser tool
server the own harness gives the engine). `ops/launchd/` has the launchd jobs for the daemon and
the watchdog.

## Talking to Hermes

swarmd does not understand messages from Telegram. Hermes forwards each one, verbatim, to the
`swarm` MCP server that swarmd serves at `http://127.0.0.1:8765/mcp` (loopback, bearer secret):

- `swarm_tell(channel, text)`: the one tool for human messages; returns an ack within 10 s.
- `swarm_projects`, `swarm_status`, `swarm_inbox`, `swarm_report`: read-only, for display.

The skill `hermes/skills/swarm-front/SKILL.md` tells Hermes to do exactly that, and
`hermes/config-snippet.yaml` is the Hermes config to merge (MCP server, skill, kanban). swarmd
sends all outbound messages itself, through its own Telegram notifier, and mirrors its projects
as kanban boards on the Hermes dashboard.

## How to add an engine

An engine is a CLI that supplies the model. Follow `src/swarm/engine/registry.py`:

1. Add a module in `engine/` with a class implementing `Engine` (`engine/base.py`).
2. Add one line to `ENGINE_FACTORIES` in `engine/registry.py`.
3. Add its `(harness, engine, browser)` rows to `COMPATIBLE` in `harness/registry.py`.
4. Add a quota cap for it. The cap is not configurable: the daemon builds `SqliteQuotaLedger`
   without `caps` (`Daemon._build_core`), so the only cap is the default `{"claude": 3}` in
   `scheduler/ledger.py`, and an engine missing from it is never leased. Add it to that default,
   or pass `caps=` where the daemon constructs the ledger.
5. Select it. The daemon builds one engine, the one named by `default_variant(cfg, browser)` in
   `daemon.py` (called from `Daemon._build_core`), and hands the pool only that engine. Until
   `default_variant` names your engine, steps 1-4 register it but nothing runs on it. Change the
   `engine` (and `model`) fields there. `swarm project create` stamps new projects with the same
   variant, so recreate or edit existing projects.

Not generic yet: the daemon's login check (`default_auth_check` in `housekeeping.py`) and the
engine settings in `Config` (`claude_binary`, `claude_model`) are claude's.

## How to add a harness

A harness is a way of doing browser work.

1. Add a module in `harness/` with a `HarnessAdapter` (`harness/base.py`).
2. Add one line to `ADAPTER_FACTORIES` in `harness/registry.py`.
3. Add its rows to `COMPATIBLE` in the same file.
4. Select it. A run uses its project's `default_variant`, which `swarm project create` copies from
   `default_variant(cfg, "chrome")` in `daemon.py` (`harness="own"` today); there is no flag to
   pick another. Change the `harness` field there, then recreate or edit existing projects.

`tests/harness/test_contract.py` runs the contract against every entry of `ADAPTER_FACTORIES`.
Stay inside the harness rules: use `engine.base` and `browser.cdp`, not concrete adapters.

## How to add a front

A front is a way to reach humans. Today there is one notifier, Telegram, and the daemon wires it
in `Daemon._build_notifier`; `_build_coordinator` and `_build_fronts` then hand it to the
Coordinator, the internal API and `SwarmFrontService`.

1. Implement `ports.Notifier` (`send`). If it announces Asks, also provide `has_ask`
   (`AskNotifier` in `front/internal_api.py`), so the announce sweep can heal missed Asks.
2. Put it in `front/` and build it in `Daemon._build_notifier`, replacing or wrapping the
   Telegram one; the daemon has a single notifier slot, so there is no fan-out yet.
3. Address it with a channel-ref scheme: `telegram:<chat_id>[:<thread_id>]` today, so a new
   front picks its own prefix.

## Running tests

```sh
uv run pytest -m "not integration and not hermes" -q && uv run ruff check src tests scripts && uv run ruff format --check src tests scripts
```

Tests marked `integration` need real Chrome and the `claude` CLI; `hermes` needs a running
Hermes. `scripts/soak.py` drives the 48-hour soak on the target Mac (see its docstring).

## Open items

- User-facing messages are in Korean; there is no i18n.
- The Hermes REST and WebSocket shapes (kanban mirror) are assumed, pending verification
  against a live Hermes.
- WebSocket and WebRTC sends are not intercepted by the action guard: a known gap.
- The login check (`default_auth_check`) is claude-specific.
- Engine/harness selection is a single default variant; per-project or per-order selection is
  not configurable yet.

## License

MIT. See `LICENSE`.
