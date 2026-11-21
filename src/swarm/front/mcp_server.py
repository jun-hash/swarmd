"""SwarmFront MCP: the only way human words reach swarmd, called by Hermes.

Five async tools, served over streamable
HTTP at `http://127.0.0.1:{internal_port}/mcp` inside the internal API's Starlette app (so
the same bearer guard applies). Hermes passes the human's text through untouched; the
Coordinator decides whether it is an order, an Ask answer or feedback.

Every tool returns within 10 s: `swarm_tell` relies on `Coordinator.handle_tell`'s own ack
timeout; status waits at most `wait_s` and apologises otherwise; a report that takes longer
than `wait_s` is promised and delivered later through the Notifier.
"""

import asyncio
import contextlib
import logging
from typing import Literal, Protocol

from mcp.server.mcpserver import MCPServer
from starlette.applications import Starlette

from swarm.ports import Notifier
from swarm.projects.store import ProjectStore
from swarm.types import AskView, ChannelRef, Outbound, ProjectView, TellResult

log = logging.getLogger(__name__)

TOOL_NAMES = ("swarm_tell", "swarm_status", "swarm_inbox", "swarm_projects", "swarm_report")
FRONT_WAIT_S = (
    8.0  # under the 10 s ack timeout (coordinator.ACK_TIMEOUT_S), leaves room for transport
)
STATUS_MAX = 3_500
REPORT_LATER = "📊 보고서를 만드는 중이에요. 끝나면 이 토픽으로 보낼게요."
REPORT_FAILED = "⚠️ 보고서를 만들지 못했어요. 잠시 후 다시 요청해 주세요."
STATUS_FAILED = "⚠️ 지금 상태를 가져오지 못했어요. 잠시 후 다시 물어봐 주세요."
TELL_FAILED = "⚠️ 메시지를 처리하지 못했어요. 다시 보내 주세요"

INSTRUCTIONS = (
    "swarmd front door. Pass the human's message to swarm_tell exactly as written (no "
    "summary, no rewording, no interpretation) together with the Telegram channel it came "
    "from, 'telegram:<chat_id>[:<thread_id>]'. Answers to questions ('#2 B로 가') also go "
    "through swarm_tell; swarmd decides what a message means. Relay the returned ack."
)


class CoordinatorPort(Protocol):
    async def handle_tell(self, channel: ChannelRef, text: str) -> TellResult: ...
    async def status(self, channel: ChannelRef) -> str: ...
    async def report(self, channel: ChannelRef, kind: Literal["daily", "weekly"]) -> str: ...


class InboxPort(Protocol):
    def open_asks(self, project_id=None) -> list[AskView]: ...
    def topic_number(self, ask_id: str) -> int: ...


class InboxEntry(AskView):
    """AskView plus the topic number the human answers with ("#2 B로 가")."""

    number: int


class SwarmFrontService:
    def __init__(
        self,
        *,
        coordinator: CoordinatorPort,
        inbox: InboxPort,
        projects: ProjectStore,
        notifier: Notifier,
        wait_s: float = FRONT_WAIT_S,
    ) -> None:
        self._coordinator = coordinator
        self._inbox = inbox
        self._projects = projects
        self._notifier = notifier
        self._wait_s = wait_s
        self._tasks: set[asyncio.Task] = set()

    async def swarm_tell(self, *, channel: ChannelRef, text: str) -> TellResult:
        try:
            return await self._coordinator.handle_tell(channel, text)
        except Exception:  # noqa: BLE001 - the human always gets an answer
            log.exception("swarm_tell failed")
            return TellResult(ack=TELL_FAILED, project_id=None, order_id=None, closed_ask_id=None)

    async def swarm_status(self, *, channel: ChannelRef) -> str:
        try:
            text = await asyncio.wait_for(self._coordinator.status(channel), self._wait_s)
        except Exception:  # noqa: BLE001 - timeout or failure: answer in time anyway
            log.warning("swarm_status failed", exc_info=True)
            return STATUS_FAILED
        return (text or "").strip()[:STATUS_MAX] or STATUS_FAILED

    async def swarm_inbox(self) -> list[InboxEntry]:
        out: list[InboxEntry] = []
        for a in self._inbox.open_asks():
            out.append(InboxEntry(**a, number=self._inbox.topic_number(a["ask_id"])))
        return out

    async def swarm_projects(self) -> list[ProjectView]:
        return [
            ProjectView(
                id=p["id"],
                slug=p["slug"],
                name=p["name"],
                channel=p["channel"],
                objective=p["goal"]["objective"],
                open_asks=len(self._inbox.open_asks(p["id"])),
            )
            for p in self._projects.list()
        ]

    async def swarm_report(
        self, *, channel: ChannelRef, kind: Literal["daily", "weekly"]
    ) -> str:  # fmt: skip
        task = asyncio.ensure_future(self._coordinator.report(channel, kind))
        try:
            return await asyncio.wait_for(asyncio.shield(task), self._wait_s)
        except TimeoutError:
            self._deliver_later(channel, task)
            return REPORT_LATER
        except asyncio.CancelledError:  # the MCP call went away: the report still arrives
            self._deliver_later(channel, task)
            raise
        except Exception:  # noqa: BLE001
            log.warning("swarm_report failed", exc_info=True)
            return REPORT_FAILED

    def _deliver_later(self, channel: ChannelRef, task: asyncio.Future) -> None:
        self._tasks.add(task)
        task.add_done_callback(lambda t: self._late_report(channel, t))

    def _late_report(self, channel: ChannelRef, task: asyncio.Task) -> None:
        self._tasks.discard(task)
        if task.cancelled():
            return
        text = REPORT_FAILED if task.exception() is not None else task.result()
        kind = "fyi" if task.exception() is not None else "report"
        send = asyncio.ensure_future(
            self._notifier.send(
                Outbound(channel=channel, kind=kind, text=text, ref_id=None, urgent=False)  # type: ignore[typeddict-item]
            )
        )
        self._tasks.add(send)
        send.add_done_callback(self._sent)

    def _sent(self, task: asyncio.Task) -> None:
        self._tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            log.error("late report not queued", exc_info=task.exception())

    async def aclose(self) -> None:
        for t in list(self._tasks):
            t.cancel()
        with contextlib.suppress(Exception):
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()


def build_mcp(front: SwarmFrontService) -> MCPServer:
    mcp = MCPServer("swarm-front", instructions=INSTRUCTIONS)

    async def swarm_tell(channel: str, text: str) -> TellResult:
        return await front.swarm_tell(channel=channel, text=text)

    async def swarm_status(channel: str) -> str:
        return await front.swarm_status(channel=channel)

    async def swarm_inbox() -> list[InboxEntry]:
        return await front.swarm_inbox()

    async def swarm_projects() -> list[ProjectView]:
        return await front.swarm_projects()

    async def swarm_report(channel: str, kind: Literal["daily", "weekly"]) -> str:
        return await front.swarm_report(channel=channel, kind=kind)

    mcp.add_tool(
        swarm_tell,
        name="swarm_tell",
        description=(
            "Hand a human message to swarmd: orders, answers to questions ('#2 B로 가') and "
            "feedback all go here. `text` must be the human's words exactly as written; "
            "`channel` is where it came from, 'telegram:<chat_id>[:<thread_id>]'. Returns a "
            "short ack to relay plus ids."
        ),
    )
    mcp.add_tool(
        swarm_status,
        name="swarm_status",
        description="Status summary of the project mapped to `channel` (all projects for an "
        "unmapped channel). <= 3,500 characters.",
    )
    mcp.add_tool(
        swarm_inbox,
        name="swarm_inbox",
        description="All open questions (Asks) waiting for a human, with their topic number.",
    )
    mcp.add_tool(
        swarm_projects,
        name="swarm_projects",
        description="Projects with their Telegram channel mapping and open question count.",
    )
    mcp.add_tool(
        swarm_report,
        name="swarm_report",
        description="Daily or weekly report for the project mapped to `channel`.",
    )
    return mcp


def streamable_app(mcp: MCPServer) -> Starlette:
    """The MCP Starlette app at /mcp (streamable HTTP). Stateless + JSON responses: our tools
    need no server-initiated messages, and a swarmd restart never strands Hermes' session."""
    return mcp.streamable_http_app(
        streamable_http_path="/mcp", stateless_http=True, json_response=True, host="127.0.0.1"
    )
