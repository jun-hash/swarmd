"""Inbox (domain layer): the Ask lifecycle, from a worker or the Coordinator raising a question
to a human answer, a policy match or a deadline closing it.

Facade: `Inbox` (`inbox.SqliteInbox`). May import shared and store.
It must never know who raised an Ask or how it reaches a human (no `front`, `workers`,
`coordinator`), and never imports `projects` or `scheduler`.
"""
