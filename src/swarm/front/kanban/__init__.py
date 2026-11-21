"""The Hermes kanban mirror: the backlog and the runs shown on the Hermes
dashboard's board, and the human's allowed edits brought back as Coordinator events.

- `cards`: backlog item / run -> card, human change -> event (pure)
- `hermes`: HermesTransport (REST + WS), the remote types and errors
- `store`: MirrorTables, every `kanban_*` table
- `mirror`: HermesKanbanMirror, which feeds, flushes, listens and resyncs

Facade: `KanbanMirror` (the mirror's Protocol), built as `HermesKanbanMirror`. Front-layer rules
apply: no `workers`; runs come in through the `RunReader` Protocol, never the pool itself.
"""

from swarm.front.kanban.hermes import HermesTransport, WsConnect
from swarm.front.kanban.mirror import HermesKanbanMirror, KanbanMirror, RunReader

__all__ = ["HermesKanbanMirror", "HermesTransport", "KanbanMirror", "RunReader", "WsConnect"]
