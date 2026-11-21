"""TelegramNotifier: swarmd speaks first.

`send()` only appends to a durable SQLite outbox and returns. `drain()` (background loop) /
`drain_once()` (tests) deliver through the Bot API `sendMessage` as plain text (never a
`parse_mode`, so `*_[` can't break a message), split into <= 3,500-unit chunks that end in
"…(계속)". Rules:

- order is kept per channel: a message waiting for a retry blocks the ones behind it;
  other channels keep going
- failures (network, 401/403/404/5xx) stay in the outbox and retry with backoff 1m, 2m,
  4m ... capped at 30m; a 429 waits its `retry_after` without growing the backoff. Only a
  400 (bad chat / thread / text) is dead-lettered so a channel is never stuck; a dead Ask
  is re-sent once to the General channel (ref `<ask_id>:fallback`) so its human, and its
  deadline, are never lost
- outside the alert window (08:00-23:00) non-urgent messages are held and go out together
  when it opens; urgent ones go at once and are not blocked by the held ones
- a non-null `ref_id` is enqueued at most once, ever (`notify.duplicate`)
- `kind="ask"` rows carry `ask_id`: skipped if the Ask closed meanwhile; after the last
  chunk is delivered, `inbox.mark_sent(ask_id, at)` starts the Ask's deadline (Review
  Focus 4). `has_ask(ask_id)` tells the announce sweep whether an Ask was ever enqueued

The bot token appears only in the request path; it is redacted from httpx logs and from
every error we store or log.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Literal, Protocol

import httpx

from swarm.aio import pause
from swarm.backoff import doubling
from swarm.clock import AlertWindow, Clock
from swarm.redact import add_secret, redact
from swarm.store.db import Database
from swarm.store.events import Emitter, EventLog
from swarm.types import AskId, AskView, ChannelRef, Outbound

log = logging.getLogger(__name__)

TEXT_MAX = 3_500  # Telegram allows 4,096 UTF-16 units; we cap ours at 3,500
CONTINUED = "\n…(계속)"
BACKOFF_BASE_S = 60
BACKOFF_CAP_S = 1_800
PERMANENT_STATUS = frozenset({400})  # bad chat / thread / text: retrying cannot help
POLL_MAX_S = 60.0
POLL_MIN_S = 1.0
KINDS = frozenset({"ask", "report", "fyi", "ack"})
GENERAL_THREAD = 1  # forum General topic: Telegram wants no message_thread_id
API_BASE = "https://api.telegram.org"

_SCHEMA = """
CREATE TABLE outbox (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  channel TEXT NOT NULL, kind TEXT NOT NULL, text TEXT NOT NULL, ref_id TEXT,
  urgent INTEGER NOT NULL, status TEXT NOT NULL,
  parts_sent INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TEXT, last_error TEXT, created_at TEXT NOT NULL, done_at TEXT);
CREATE UNIQUE INDEX outbox_ref ON outbox(ref_id) WHERE ref_id IS NOT NULL;
CREATE INDEX outbox_pending ON outbox(status, seq)
"""

_SCHEMA_V2 = """
ALTER TABLE outbox ADD COLUMN ask_id TEXT;
UPDATE outbox SET ask_id = ref_id WHERE kind = 'ask';
CREATE INDEX outbox_ask ON outbox(ask_id)
"""
FALLBACK_SUFFIX = ":fallback"
FALLBACK_NOTE = "\n(프로젝트 토픽으로 보내지 못해 여기로 보냈어요)"
REJECT_DESC_MAX = 300  # Telegram's error description in a rejection reason


class AskSink(Protocol):
    """The two Inbox calls the notifier makes."""

    def open_asks(self, project_id=None) -> list[AskView]: ...
    def mark_sent(self, ask_id: AskId, at: datetime) -> None: ...


def _units(s: str) -> int:
    """Length as Telegram counts it (UTF-16 code units)."""
    return len(s.encode("utf-16-le")) // 2


def _fit(s: str, room: int) -> int:
    """Longest prefix (in characters) of s that is at most `room` UTF-16 units."""
    used = 0
    for i, ch in enumerate(s):
        used += 2 if ord(ch) > 0xFFFF else 1
        if used > room:
            return i
    return len(s)


def split_text(text: str, limit: int = TEXT_MAX) -> list[str]:
    """Chunks of <= limit units; every chunk but the last ends with CONTINUED. Cuts after a
    newline when one is in the second half of the chunk, else hard."""
    parts: list[str] = []
    room = limit - _units(CONTINUED)
    rest = text
    while _units(rest) > limit:
        cut = max(_fit(rest, room), 1)
        nl = rest.rfind("\n", 0, cut)
        if nl + 1 > cut // 2:
            cut = nl + 1
        parts.append(rest[:cut] + CONTINUED)
        rest = rest[cut:]
    parts.append(rest)
    return parts


def parse_channel(channel: ChannelRef) -> tuple[str, int | None]:
    """ "telegram:<chat_id>[:<thread_id>]" -> (chat_id, thread_id or None)."""
    kind, _, rest = channel.partition(":")
    bits = rest.split(":")
    if kind != "telegram" or not bits[0] or len(bits) > 2:
        raise ValueError(f"not a telegram channel: {channel!r}")
    chat = bits[0]
    if not (chat.lstrip("-").isdigit() or chat.startswith("@")):
        raise ValueError(f"bad chat id in {channel!r}")
    if len(bits) == 1:
        return chat, None
    if not bits[1].isdigit():
        raise ValueError(f"bad thread id in {channel!r}")
    thread = int(bits[1])
    return chat, None if thread == GENERAL_THREAD else thread


Verdict = Literal["sent", "retry", "dead", "skipped"]


class TelegramNotifier:
    def __init__(
        self,
        token: str,
        db: Database,
        inbox: AskSink,
        clock: Clock,
        alert_window: AlertWindow | None = None,
        *,
        log: EventLog | None = None,
        general_channel: ChannelRef | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        api_base: str = API_BASE,
        timeout_s: float = 15.0,
    ) -> None:
        if not token:
            raise ValueError("telegram bot token is empty")
        self._token = token
        self._db = db
        self._inbox = inbox
        self._clock = clock
        self._window = alert_window or AlertWindow()
        self._emit = Emitter(log, clock, "notifier", best_effort=True)
        self._general = general_channel  # where a dead-lettered Ask is re-sent
        self._http = httpx.AsyncClient(base_url=api_base, transport=transport, timeout=timeout_s)
        self._lock = asyncio.Lock()  # one drain at a time (loop + tests / shutdown flush)
        self._wake = asyncio.Event()
        add_secret(token)
        db.migrate("front_outbox_v1", _SCHEMA)
        db.migrate("front_outbox_v2", _SCHEMA_V2)

    # --- plumbing ------------------------------------------------------------------------

    def _set(self, seq: int, **cols) -> None:
        sets = ", ".join(f"{k}=?" for k in cols)
        self._db.execute(f"UPDATE outbox SET {sets} WHERE seq=?", (*cols.values(), seq))

    # --- Notifier port -------------------------------------------------------------------

    async def send(self, msg: Outbound) -> None:
        """Append to the outbox and return; delivery is asynchronous."""
        if msg["kind"] not in KINDS:
            raise ValueError(f"unknown outbound kind {msg['kind']!r}")
        if not msg["text"].strip():
            raise ValueError("empty outbound text")
        parse_channel(msg["channel"])  # fail fast on a channel we could never deliver to
        ref = msg.get("ref_id")
        self._enqueue(
            msg["channel"], msg["kind"], msg["text"], ref, bool(msg.get("urgent")),
            ask_id=ref if msg["kind"] == "ask" else None,
        )  # fmt: skip

    def _enqueue(
        self, channel: ChannelRef, kind: str, text: str, ref: str | None, urgent: bool,
        *, ask_id: str | None,
    ) -> bool:  # fmt: skip
        cur = self._db.execute(
            "INSERT OR IGNORE INTO outbox(channel, kind, text, ref_id, urgent, status,"
            " created_at, ask_id) VALUES (?,?,?,?,?,'pending',?,?)",
            (channel, kind, text, ref, 1 if urgent else 0, self._clock.now().isoformat(), ask_id),
        )
        if not cur.rowcount:
            self._emit("notify.duplicate", {"ref_id": ref, "kind": kind})
            return False
        self._wake.set()
        return True

    def has_ask(self, ask_id: AskId) -> bool:
        """Was this Ask ever enqueued (any status, fallback included)?"""
        return (
            self._db.execute("SELECT 1 FROM outbox WHERE ask_id=? LIMIT 1", (ask_id,)).fetchone()
            is not None
        )

    # --- delivery ------------------------------------------------------------------------

    def _pending(self) -> list:
        return self._db.execute(
            "SELECT * FROM outbox WHERE status='pending' ORDER BY seq"
        ).fetchall()

    async def drain_once(self) -> int:
        """Deliver everything that is due now. Returns how many messages completed."""
        async with self._lock:
            now = self._clock.now()
            blocked: set[str] = set()
            done = 0
            for r in self._pending():
                ch = r["channel"]
                if ch in blocked:
                    continue
                if not r["urgent"] and not self._window.is_open(now):
                    continue  # held for the window; does not block urgent ones behind it
                due = r["next_attempt_at"]
                if due is not None and datetime.fromisoformat(due) > now:
                    blocked.add(ch)  # keep the channel's order: nothing overtakes a retry
                    continue
                try:
                    verdict = await self._deliver(r)
                except Exception as e:  # noqa: BLE001 - a bug in one row never stops the drain
                    error = redact(repr(e))  # no traceback: it could carry the token
                    log.error("outbox delivery crashed (seq %s): %s", r["seq"], error)
                    verdict = self._retry(r, error)
                if verdict == "retry":
                    blocked.add(ch)
                elif verdict == "sent":
                    done += 1
            return done

    def _ask_closed(self, ask_id: str) -> bool:
        try:
            return ask_id not in {a["ask_id"] for a in self._inbox.open_asks()}
        except Exception:  # noqa: BLE001 - when unsure, deliver the question
            log.warning("open_asks failed; sending ask %s anyway", ask_id, exc_info=True)
            return False

    async def _deliver(self, r) -> Verdict:
        seq, ask_id = r["seq"], r["ask_id"]
        if ask_id and self._ask_closed(ask_id):
            self._set(seq, status="skipped", done_at=self._clock.now().isoformat())
            self._emit(
                "notify.skipped",
                {"seq": seq, "ref_id": r["ref_id"], "ask_id": ask_id, "reason": "ask closed"},
            )
            return "skipped"
        try:
            chat, thread = parse_channel(r["channel"])
        except ValueError as e:
            return self._dead(r, None, str(e))
        parts = split_text(r["text"])
        for i in range(r["parts_sent"], len(parts)):
            status, retry_after, error = await self._post(chat, thread, parts[i])
            if status == 200:
                self._set(seq, parts_sent=i + 1)
                continue
            if status in PERMANENT_STATUS:
                return self._dead(r, status, error)
            return self._retry(r, error, retry_after)
        at = self._clock.now()
        self._set(seq, status="sent", done_at=at.isoformat(), next_attempt_at=None)
        if ask_id:
            try:
                self._inbox.mark_sent(ask_id, at)
            except Exception:  # noqa: BLE001 - delivered is delivered; never resend
                log.warning("mark_sent failed for %s", ask_id, exc_info=True)
                self._emit("notify.mark_sent_failed", {"seq": seq, "ask_id": ask_id})
        return "sent"

    async def _post(
        self, chat: str, thread: int | None, text: str
    ) -> tuple[int | None, float | None, str]:
        """-> (HTTP status or None on a transport error, retry_after seconds, error text)."""
        body: dict = {"chat_id": chat, "text": text}
        if thread is not None:
            body["message_thread_id"] = thread
        try:
            resp = await self._http.post(f"/bot{self._token}/sendMessage", json=body)
        except httpx.HTTPError as e:
            return None, None, f"{type(e).__name__}: {redact(str(e))}"
        try:
            data = resp.json()
        except ValueError:
            data = {}
        if resp.status_code == 200 and data.get("ok"):
            return 200, None, ""
        status = resp.status_code if resp.status_code != 200 else 502  # 200 without ok
        params = data.get("parameters") or {}
        retry_after = params.get("retry_after") if isinstance(params, dict) else None
        desc = redact(str(data.get("description") or resp.reason_phrase))[:REJECT_DESC_MAX]
        return status, retry_after, f"HTTP {status}: {desc}"

    def _retry(self, r, error: str, retry_after: float | None = None) -> Verdict:
        if retry_after:  # rate limited (429): wait what Telegram asks, backoff unchanged
            attempts, wait = r["attempts"], float(retry_after)
        else:
            attempts = r["attempts"] + 1
            wait = doubling(attempts, BACKOFF_BASE_S, BACKOFF_CAP_S)
        nxt = self._clock.now() + timedelta(seconds=wait)
        self._set(r["seq"], attempts=attempts, next_attempt_at=nxt.isoformat(), last_error=error)
        log.warning("telegram send failed (seq %s, try %d), retry in %ss: %s",
                    r["seq"], attempts, wait, error)  # fmt: skip
        self._emit(
            "notify.retry",
            {"seq": r["seq"], "channel": r["channel"], "kind": r["kind"], "ref_id": r["ref_id"],
             "attempt": attempts, "wait_s": wait, "error": error},
        )  # fmt: skip
        return "retry"

    def _dead(self, r, status: int | None, error: str) -> Verdict:
        self._set(r["seq"], status="dead", done_at=self._clock.now().isoformat(), last_error=error)
        log.error("telegram message dead-lettered (seq %s): %s", r["seq"], error)
        self._emit(
            "notify.dead",
            {"seq": r["seq"], "channel": r["channel"], "kind": r["kind"], "ref_id": r["ref_id"],
             "status": status, "error": error},
        )  # fmt: skip
        if r["ask_id"]:
            self._fall_back(r)
        return "dead"

    def _fall_back(self, r) -> None:
        """A dead Ask would never be marked sent (no deadline, no human): re-send it once to
        General. The fallback row keeps the original ask_id, so its delivery marks it sent."""
        ref = r["ref_id"] or r["ask_id"]
        if self._general is None or r["channel"] == self._general or ref.endswith(FALLBACK_SUFFIX):
            return  # nowhere better to go (already General, or already the fallback)
        fb = ref + FALLBACK_SUFFIX
        if self._enqueue(
            self._general, r["kind"], r["text"] + FALLBACK_NOTE, fb, bool(r["urgent"]),
            ask_id=r["ask_id"],
        ):  # fmt: skip
            self._emit(
                "notify.fallback",
                {"seq": r["seq"], "ask_id": r["ask_id"], "ref_id": fb, "from": r["channel"],
                 "to": self._general},
            )  # fmt: skip

    # --- loop ----------------------------------------------------------------------------

    def next_due(self) -> datetime | None:
        """Earliest time a pending message may be attempted (a wake-up hint for drain)."""
        now = self._clock.now()
        floor: dict[str, datetime] = {}  # a row waiting for a retry holds back its channel
        best: datetime | None = None
        for r in self._pending():
            ch = r["channel"]
            nxt = datetime.fromisoformat(r["next_attempt_at"]) if r["next_attempt_at"] else None
            at = max(nxt or now, floor.get(ch, now))
            if not r["urgent"] and not self._window.is_open(at):
                at = self._window.next_open(at)
            best = at if best is None else min(best, at)
            if nxt is not None:
                floor[ch] = max(floor.get(ch, nxt), nxt)
        return best

    async def drain(self, stop: asyncio.Event | None = None) -> None:
        """Background delivery: wakes on send(), when the next message is due, or every 60 s."""
        stop = stop or asyncio.Event()
        while not stop.is_set():
            self._wake.clear()
            wait = POLL_MAX_S
            try:
                await self.drain_once()
                due = self.next_due()
                if due is not None:
                    left = (due - self._clock.now()).total_seconds()
                    wait = min(max(left, POLL_MIN_S), POLL_MAX_S)
            except Exception as e:  # noqa: BLE001 - the loop outlives any one failure (DB busy)
                log.error("outbox drain failed: %s", redact(repr(e)))
            await pause(wait, stop, self._wake)

    async def aclose(self) -> None:
        await self._http.aclose()
