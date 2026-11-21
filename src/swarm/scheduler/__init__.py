"""Scheduler (domain layer): the quota ledger that decides when an engine may be used.

Facade: `QuotaLedger.lease` (`ledger.SqliteQuotaLedger`); a lease is per engine, parallel-capped,
and an engine cools down after a usage limit or is disabled on an auth failure.
May import shared and store.
It must never know what work holds a lease or which engine binary runs it (no `engine`,
`browser` or `harness`), and never imports `projects` or `inbox`.
"""
