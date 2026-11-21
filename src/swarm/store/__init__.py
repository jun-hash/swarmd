"""Store (below every domain package): SQLite and files, with no knowledge of what is stored.

Facade: `db.Database` (one WAL connection, re-entrant transactions, named migrations), with
`events`, `blobs`, `kv` and `codec` as helpers built on it. May import the shared layer only.
It must never know who uses it: packages create their own tables through `Database.migrate`,
and `store` imports no package above it.
"""
