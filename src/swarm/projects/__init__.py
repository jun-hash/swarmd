"""Projects (domain layer): the objectives, key results and backlog the daemon works toward.

Facade: `ProjectStore` (`store.SqliteProjectStore`). `files` renders projects to files and
ingests human edits back; `render` holds the pure markdown renderers.
May import shared and store. It must never know engines, browsers or harnesses, and it never
imports `inbox` or `scheduler`: domain packages share only the Protocols in `ports`.
"""
