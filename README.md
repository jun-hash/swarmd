# swarmd

A daemon that does browser work for you.

You give it goals over Telegram. It keeps a backlog per project, drives Chrome over CDP using
the `claude` CLI, checks each result against evidence, and asks you before doing anything that
changes the outside world.

One Python 3.12 asyncio process. Runs on macOS. Pre-release.

## How it works

```
Telegram -> Hermes -> swarmd (MCP) -> coordinator -> workers -> harness -> claude + Chrome
                         |
                         +-> Telegram notifier, Hermes kanban
```

[Hermes](https://github.com/NousResearch/hermes-agent) receives your messages and forwards them
unchanged to swarmd's MCP server. swarmd decides what to do: start work, answer a pending
question, or update a goal. It replies through its own Telegram bot.

## Quickstart

You need macOS, Chrome, a logged-in `claude` CLI, a Telegram bot in a forum group, and
[uv](https://docs.astral.sh/uv/).

```sh
uv sync
uv run swarm init --telegram-token <token> --general-channel telegram:<chat_id>:1 \
    --profile work=<chrome-user-data-dir>
uv run swarm doctor
uv run swarm project create --slug compet --name "Competitor watch" \
    --channel telegram:<chat_id>:<thread_id> --objective "Track competitor pricing" \
    --kr "kr1|pages checked|20|weekly"
uv run swarmd
uv run swarm tell telegram:<chat_id>:<thread_id> "check the pricing page today"
```

`swarm init` writes `~/swarm/config.toml` and a secret. Pass `--config PATH` to use another file.

To run it permanently, install the launchd jobs in `ops/launchd/` (instructions inside).

## Hermes setup

swarmd serves MCP at `http://127.0.0.1:8765/mcp` behind a bearer secret.

- `swarm_tell(channel, text)` takes every human message.
- `swarm_projects`, `swarm_status`, `swarm_inbox` and `swarm_report` are read-only.

Copy `hermes/skills/swarm-front/` into your Hermes skills and merge
`hermes/config-snippet.yaml` into your Hermes config.

## Code layout

Layers, top to bottom. A module imports only its own layer or lower ones.
`tests/test_layers.py` enforces this.

| Layer | Packages |
|---|---|
| process | `daemon`, `watchdog`, `cli`, `housekeeping` |
| front | `front` |
| orchestration | `coordinator`, `workers` |
| domain | `projects`, `inbox`, `scheduler` |
| adapters | `harness`, `engine`, `browser` |
| store | `store` |
| shared | `types`, `ports`, `config`, `clock`, ... |

Only `daemon` builds concrete adapters. Every other layer depends on Protocols. Each package's
`__init__.py` states what it may import.

## Extending

Three swap points, each with a registry:

- **Engine** (the model CLI): implement `engine/base.py:Engine` and register it in
  `engine/registry.py`.
- **Harness** (how browser work is done): implement `harness/base.py:HarnessAdapter` and
  register it in `harness/registry.py`. `tests/harness/test_contract.py` checks every adapter.
- **Front** (how humans are reached): implement `ports.Notifier` and wire it in
  `Daemon._build_notifier`.

For a new engine or harness, add its row to `COMPATIBLE` in `harness/registry.py` and select it
in `default_variant` in `daemon.py`. A new engine also needs a quota cap in
`scheduler/ledger.py`.

## Tests

```sh
uv run pytest -m "not integration and not hermes"
uv run ruff check src tests scripts
```

`integration` tests need Chrome and `claude`. `hermes` tests need a running Hermes.
`scripts/soak.py` runs a 48-hour soak test.

## Limitations

- Messages to users are in Korean only.
- The Hermes kanban API shapes are not yet verified against a live Hermes.
- The action guard does not see WebSocket or WebRTC traffic.
- Only `claude` is supported as an engine today. One engine and harness are used for all projects.

## License

MIT
