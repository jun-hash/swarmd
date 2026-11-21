"""Front (the outermost package before the process layer): how humans and Hermes reach swarmd.

Facade: `SwarmFrontService` (`mcp_server`), behind the MCP tools Hermes calls; `internal_api`
serves workers and the watchdog, `notifier` speaks to Telegram, `kanban` mirrors the board.
May import the Coordinator facade, inbox, projects, `ports`, store and shared. It must never
know workers, browsers, engines or harnesses; it only talks to the Coordinator and the domain.
"""
