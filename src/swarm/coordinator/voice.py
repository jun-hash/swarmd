"""Voice: how the Coordinator talks to humans through the Notifier.

A reply to the human's own message is always urgent; quiet hours hold only what swarmd
starts itself.
"""

import contextlib
import logging

from swarm.ports import Notifier
from swarm.projects.store import ProjectStore
from swarm.store.events import Emitter
from swarm.types import ChannelRef, Outbound, ProjectId

log = logging.getLogger(__name__)


class Voice:
    def __init__(
        self, notifier: Notifier, projects: ProjectStore, general: ChannelRef, emit: Emitter
    ) -> None:
        self._notifier = notifier
        self._projects = projects
        self._general = general
        self._emit = emit

    async def say(
        self, channel: ChannelRef, kind: str, text: str, *, ref_id: str | None = None,
        urgent: bool = False,
    ) -> bool:  # fmt: skip
        try:
            await self._notifier.send(
                Outbound(channel=channel, kind=kind, text=text, ref_id=ref_id, urgent=urgent)  # type: ignore[typeddict-item]
            )
        except Exception as e:  # noqa: BLE001 - the outbox owns retries; never break a tick
            log.warning("notify failed", exc_info=True)
            self._emit("coordinator.notify_failed", channel=channel, error=repr(e))
            return False
        return True

    async def reply(self, channel: ChannelRef, kind: str, text: str) -> bool:
        """A reply to the human's own message (ack, late reply, "처리하지 못했어요"): always
        urgent, so quiet hours never hold it (they are for what swarmd starts)."""
        return await self.say(channel, kind, text, urgent=True)

    def channel_of(self, project_id: ProjectId | None) -> ChannelRef:
        if project_id:
            with contextlib.suppress(KeyError):
                return self._projects.get(project_id)["channel"]
        return self._general
