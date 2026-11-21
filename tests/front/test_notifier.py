"""TelegramNotifier: durable outbox, plain text, chunks,
quiet hours, per-channel order, 1m->30m backoff, ref_id dedupe, Ask mark_sent."""

import asyncio
import logging
from datetime import datetime, time, timedelta

import httpx
import pytest

from swarm.clock import SEOUL, AlertWindow, FakeClock
from swarm.front.notifier import (
    CONTINUED,
    TEXT_MAX,
    TelegramNotifier,
    parse_channel,
    split_text,
)
from swarm.store.db import Database

from .conftest import CH, DAY, TOKEN, out

NIGHT = datetime(2026, 11, 5, 2, 30, tzinfo=SEOUL)


def varied(n: int) -> str:
    return "".join(chr(0xAC00 + i % 997) for i in range(n))


GENERAL = "telegram:-100123:1"


def make(db, inbox, clock, fake_tg, **kw) -> TelegramNotifier:
    kw.setdefault("general_channel", GENERAL)
    return TelegramNotifier(
        TOKEN, db, inbox, clock, AlertWindow(time(8, 0), time(23, 0)),
        transport=fake_tg.transport, **kw,
    )  # fmt: skip


@pytest.fixture
def notifier(db, inbox_spy, clock, fake_tg, log):
    return make(db, inbox_spy, clock, fake_tg, log=log)


@pytest.fixture
def clock_at_0230():
    return FakeClock(NIGHT)


# --- special characters, long bodies ------------------------------------


async def test_long_text_is_split_plain(notifier, fake_tg):
    await notifier.send(out(text="*_[`" + "가" * 8000))
    await notifier.drain_once()
    assert all(len(m["text"]) <= 3500 and "parse_mode" not in m for m in fake_tg.sent)
    assert len(fake_tg.sent) >= 3


async def test_chunks_carry_continued_marker_and_rejoin(notifier, fake_tg):
    text = "*_[`\n" + "\n".join(f"{i:04d} " + "나" * 60 for i in range(200))
    await notifier.send(out(text=text))
    await notifier.drain_once()
    parts = fake_tg.texts()
    assert all(p.endswith(CONTINUED) for p in parts[:-1]) and not parts[-1].endswith(CONTINUED)
    assert "".join(p.removesuffix(CONTINUED) for p in parts) == text
    assert parts[0].startswith("*_[`")  # markdown characters arrive untouched


def test_split_counts_utf16_units():
    # Telegram counts UTF-16 code units: an emoji is 2, so 3,500 emoji need 2+ messages
    parts = split_text("😀" * 3500)
    assert len(parts) >= 2
    assert all(len(p.encode("utf-16-le")) // 2 <= TEXT_MAX for p in parts)


def test_split_short_text_is_one_part():
    assert split_text("짧은 글") == ["짧은 글"]
    assert split_text("x" * TEXT_MAX) == ["x" * TEXT_MAX]


# --- quiet hours ---------------------------------------------------------------------


async def test_quiet_hours_hold_non_urgent(db, inbox_spy, clock_at_0230, fake_tg):
    n = make(db, inbox_spy, clock_at_0230, fake_tg)
    await n.send(out(text="ℹ️ FYI · 하나"))
    await n.send(out(text="📊 보고 · 둘", kind="report", ref_id="report:daily:all:2026-11-05"))
    await n.drain_once()
    assert fake_tg.sent == []
    clock_at_0230.advance(timedelta(hours=5, minutes=29))  # 07:59
    await n.drain_once()
    assert fake_tg.sent == []
    clock_at_0230.advance(timedelta(minutes=1))  # 08:00: the held batch goes, in order
    await n.drain_once()
    assert fake_tg.texts() == ["ℹ️ FYI · 하나", "📊 보고 · 둘"]


async def test_quiet_hours_start_at_23(db, inbox_spy, fake_tg):
    clock = FakeClock(datetime(2026, 11, 5, 23, 0, tzinfo=SEOUL))
    n = make(db, inbox_spy, clock, fake_tg)
    await n.send(out())
    await n.drain_once()
    assert fake_tg.sent == []
    assert n.next_due() == datetime(2026, 11, 6, 8, 0, tzinfo=SEOUL)


async def test_urgent_goes_at_night(db, inbox_spy, clock_at_0230, fake_tg):
    n = make(db, inbox_spy, clock_at_0230, fake_tg)
    inbox_spy.open_ids.add("ask_1")
    await n.send(out(text="ℹ️ FYI · 아침에", urgent=False))
    await n.send(out(text="🔐 승인 · 막혔어요", kind="ask", ref_id="ask_1", urgent=True))
    await n.drain_once()
    assert fake_tg.texts() == ["🔐 승인 · 막혔어요"]  # the held FYI does not block it
    assert inbox_spy.marked == [("ask_1", NIGHT)]


def test_alert_window_wraps_midnight():
    w = AlertWindow(time(22, 0), time(6, 0))
    assert w.is_open(datetime(2026, 11, 5, 23, 0, tzinfo=SEOUL))
    assert w.is_open(datetime(2026, 11, 6, 5, 59, tzinfo=SEOUL))
    assert not w.is_open(datetime(2026, 11, 6, 12, 0, tzinfo=SEOUL))
    assert w.next_open(datetime(2026, 11, 6, 12, 0, tzinfo=SEOUL)) == datetime(
        2026, 11, 6, 22, 0, tzinfo=SEOUL
    )


# --- order + backoff -----------------------------------------------------------------


async def test_order_preserved_and_retry_backoff(notifier, fake_tg, clock, log):
    fake_tg.fail = [500] * 7
    await notifier.send(out(text="A"))
    await notifier.send(out(text="B"))
    await notifier.drain_once()
    assert len(fake_tg.attempts) == 1 and fake_tg.sent == []  # B waits behind A
    waits = []
    for _ in range(6):
        due = notifier.next_due()
        waits.append(due - clock.now())
        clock.advance(due - clock.now() - timedelta(seconds=1))
        before = len(fake_tg.attempts)
        await notifier.drain_once()
        assert len(fake_tg.attempts) == before  # not due yet: no attempt
        clock.advance(timedelta(seconds=1))
        await notifier.drain_once()
    assert waits == [timedelta(minutes=m) for m in (1, 2, 4, 8, 16, 30)]
    assert notifier.next_due() - clock.now() == timedelta(minutes=30)  # capped
    clock.advance(timedelta(minutes=30))
    await notifier.drain_once()
    assert fake_tg.texts() == ["A", "B"]
    kinds = [e["kind"] for e in log.query(kinds=["notify.retry"])]
    assert len(kinds) == 7


async def test_network_error_is_retried(notifier, fake_tg, clock):
    fake_tg.fail = [httpx.ConnectError("down")]
    await notifier.send(out(text="A"))
    await notifier.drain_once()
    assert fake_tg.sent == []
    clock.advance(timedelta(minutes=1))
    await notifier.drain_once()
    assert fake_tg.texts() == ["A"]


async def test_429_honours_retry_after(notifier, fake_tg, clock):
    fake_tg.fail = ["429:300"]
    await notifier.send(out(text="A"))
    await notifier.drain_once()
    assert notifier.next_due() - clock.now() == timedelta(seconds=300)


async def test_other_channels_are_not_blocked(notifier, fake_tg):
    fake_tg.fail = [500]
    await notifier.send(out(text="A", channel="telegram:-100123:42"))
    await notifier.send(out(text="C", channel="telegram:-100999"))
    await notifier.drain_once()
    assert fake_tg.texts() == ["C"]


async def test_permanent_error_is_dead_lettered(notifier, fake_tg, log):
    fake_tg.fail = [400]
    await notifier.send(out(text="A"))
    await notifier.send(out(text="B"))
    await notifier.drain_once()
    assert fake_tg.texts() == ["B"]  # a bad message never blocks its channel forever
    assert [e["payload"]["status"] for e in log.query(kinds=["notify.dead"])] == [400]
    await notifier.drain_once()
    assert fake_tg.texts() == ["B"]  # a non-ask dead letter has no fallback


async def test_403_is_retried_not_dead_lettered(notifier, fake_tg, clock):
    fake_tg.fail = [403]  # e.g. bot briefly removed: stays in the outbox
    await notifier.send(out(text="A"))
    await notifier.send(out(text="B"))
    await notifier.drain_once()
    assert fake_tg.sent == []
    clock.advance(timedelta(minutes=1))
    await notifier.drain_once()
    assert fake_tg.texts() == ["A", "B"]


async def test_dead_ask_falls_back_to_general(notifier, fake_tg, inbox_spy, clock, log):
    inbox_spy.open_ids.add("ask_1")
    fake_tg.fail = [400]  # e.g. the project topic was deleted
    await notifier.send(out(text="🙋 결정 · #1 A?", kind="ask", ref_id="ask_1", channel=CH))
    await notifier.drain_once()
    assert fake_tg.sent == [] and inbox_spy.marked == []
    await notifier.drain_once()
    [m] = fake_tg.sent
    assert m["chat_id"] == "-100123" and "message_thread_id" not in m  # General
    assert m["text"].startswith("🙋 결정 · #1 A?")
    assert inbox_spy.marked == [("ask_1", clock.now())]  # the ORIGINAL ask's deadline starts
    assert notifier.has_ask("ask_1")
    [fb] = log.query(kinds=["notify.fallback"])
    assert fb["payload"]["ask_id"] == "ask_1" and fb["payload"]["ref_id"] == "ask_1:fallback"


async def test_dead_fallback_does_not_loop(notifier, fake_tg, inbox_spy, log):
    inbox_spy.open_ids.add("ask_1")
    fake_tg.fail = [400, 400]
    await notifier.send(out(kind="ask", ref_id="ask_1", channel=CH))
    for _ in range(3):
        await notifier.drain_once()
    assert fake_tg.sent == [] and len(fake_tg.attempts) == 2
    assert len(list(log.query(kinds=["notify.fallback"]))) == 1


async def test_dead_ask_on_general_has_no_fallback(notifier, fake_tg, inbox_spy):
    inbox_spy.open_ids.add("ask_1")
    fake_tg.fail = [400]
    await notifier.send(out(kind="ask", ref_id="ask_1", channel=GENERAL))
    await notifier.drain_once()
    await notifier.drain_once()
    assert len(fake_tg.attempts) == 1


async def test_429_does_not_inflate_the_backoff(notifier, fake_tg, clock):
    fake_tg.fail = ["429:5", "429:5", "429:5", 500]  # an 08:00 burst, then a real failure
    await notifier.send(out(text="A"))
    for _ in range(3):
        await notifier.drain_once()
        assert notifier.next_due() - clock.now() == timedelta(seconds=5)
        clock.advance(timedelta(seconds=5))
    await notifier.drain_once()
    assert notifier.next_due() - clock.now() == timedelta(minutes=1)  # first real failure


async def test_crash_log_is_redacted(notifier, fake_tg, caplog):
    caplog.set_level(logging.DEBUG)

    fake_tg.fail = [RuntimeError(f"bad url https://api.telegram.org/bot{TOKEN}/sendMessage")]
    await notifier.send(out(text="A"))
    await notifier.drain_once()
    assert "outbox delivery crashed" in caplog.text and TOKEN not in caplog.text


async def test_drain_loop_survives_next_due_failure(notifier, fake_tg, monkeypatch):
    import swarm.front.notifier as mod

    monkeypatch.setattr(mod, "POLL_MAX_S", 0.05)
    calls = {"n": 0}
    real = notifier.next_due

    def flaky():
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("database is locked")
        return real()

    notifier.next_due = flaky  # type: ignore[method-assign]
    stop = asyncio.Event()
    task = asyncio.create_task(notifier.drain(stop))
    await asyncio.sleep(0.1)
    assert not task.done()
    await notifier.send(out(text="A"))
    for _ in range(100):
        if fake_tg.sent:
            break
        await asyncio.sleep(0.01)
    assert fake_tg.texts() == ["A"]
    stop.set()
    await asyncio.wait_for(task, 2)


async def test_has_ask(notifier):
    assert not notifier.has_ask("ask_1")
    await notifier.send(out(kind="ask", ref_id="ask_1"))
    assert notifier.has_ask("ask_1") and not notifier.has_ask("ask_2")


async def test_failed_chunk_resumes_where_it_stopped(notifier, fake_tg, clock):
    await notifier.send(out(text=varied(8000)))
    real = fake_tg.handle
    calls = {"n": 0}

    def flaky(request):
        calls["n"] += 1
        if calls["n"] == 2:
            return httpx.Response(502, json={"ok": False, "error_code": 502})
        return real(request)

    fake_tg.transport.handler = flaky
    await notifier.drain_once()
    assert len(fake_tg.sent) == 1
    clock.advance(timedelta(minutes=1))
    await notifier.drain_once()
    parts = fake_tg.texts()
    assert len(parts) == 3 and len(set(parts)) == 3  # chunk 1 was not sent again


# --- dedupe + Asks -------------------------------------------------------------------


async def test_same_ref_id_not_sent_twice(notifier, fake_tg):
    await notifier.send(out(text="✅ 완료 · 끝", kind="report", ref_id="wo_1"))
    await notifier.send(out(text="✅ 완료 · 끝", kind="report", ref_id="wo_1"))
    await notifier.drain_once()
    await notifier.send(out(text="✅ 완료 · 또", kind="report", ref_id="wo_1"))
    await notifier.drain_once()
    assert fake_tg.texts() == ["✅ 완료 · 끝"]


async def test_messages_without_ref_are_never_deduped(notifier, fake_tg):
    await notifier.send(out(text="같은 말"))
    await notifier.send(out(text="같은 말"))
    await notifier.drain_once()
    assert fake_tg.texts() == ["같은 말", "같은 말"]


async def test_ask_send_marks_sent(notifier, inbox_spy, fake_tg, clock):
    inbox_spy.open_ids.add("ask_1")
    await notifier.send(out(text="🙋 결정 · #1 A?", kind="ask", ref_id="ask_1"))
    await notifier.send(out(text="ℹ️ FYI · 무관", ref_id="wo_9"))
    await notifier.drain_once()
    assert inbox_spy.marked == [("ask_1", clock.now())]


async def test_ask_marked_only_after_its_last_chunk(notifier, inbox_spy, fake_tg, clock):
    inbox_spy.open_ids.add("ask_1")
    real, calls = fake_tg.handle, {"n": 0}

    def flaky(request):
        calls["n"] += 1
        return httpx.Response(502, json={"ok": False}) if calls["n"] == 2 else real(request)

    fake_tg.transport.handler = flaky
    await notifier.send(out(text="가" * 5000, kind="ask", ref_id="ask_1"))
    await notifier.drain_once()
    assert inbox_spy.marked == []
    clock.advance(timedelta(minutes=1))
    await notifier.drain_once()
    assert inbox_spy.marked == [("ask_1", clock.now())]


async def test_closed_ask_is_not_sent(notifier, inbox_spy, fake_tg, log):
    # held overnight, answered / expired meanwhile: no stale question at 08:00
    await notifier.send(out(text="🙋 결정 · #1 A?", kind="ask", ref_id="ask_gone"))
    await notifier.drain_once()
    assert fake_tg.sent == [] and inbox_spy.marked == []
    assert len(list(log.query(kinds=["notify.skipped"]))) == 1


async def test_mark_sent_failure_does_not_resend(notifier, inbox_spy, fake_tg):
    inbox_spy.open_ids.add("ask_1")

    def boom(ask_id, at):
        raise KeyError(ask_id)

    inbox_spy.mark_sent = boom
    await notifier.send(out(kind="ask", ref_id="ask_1"))
    await notifier.drain_once()
    await notifier.drain_once()
    assert len(fake_tg.sent) == 1


# --- durability ----------------------------------------------------------------------


async def test_outbox_survives_restart(tmp_path, inbox_spy, clock, fake_tg):
    db = Database(tmp_path / "a.db")
    fake_tg.fail = [500]
    first = make(db, inbox_spy, clock, fake_tg)
    await first.send(out(text="A"))
    await first.send(out(text="B"))
    await first.drain_once()
    await first.aclose()
    db.close()
    db2 = Database(tmp_path / "a.db")
    second = make(db2, inbox_spy, clock, fake_tg)
    clock.advance(timedelta(minutes=1))  # the backoff survives the restart too
    await second.drain_once()
    assert fake_tg.texts() == ["A", "B"]
    await second.send(out(text="C"))
    await second.drain_once()
    assert fake_tg.texts() == ["A", "B", "C"]


async def test_send_rejects_unknown_kind(notifier):
    with pytest.raises(ValueError):
        await notifier.send(out(kind="spam"))


# --- wire format ---------------------------------------------------------------------


def test_parse_channel():
    assert parse_channel("telegram:-100123:42") == ("-100123", 42)
    assert parse_channel("telegram:-100123") == ("-100123", None)
    assert parse_channel("telegram:-100123:1") == ("-100123", None)  # General: no thread id
    with pytest.raises(ValueError):
        parse_channel("slack:C0")


async def test_send_message_wire_format(notifier, fake_tg):
    await notifier.send(out(text="hi", channel=CH))
    await notifier.send(out(text="dm", channel="telegram:5551234"))
    await notifier.drain_once()
    assert fake_tg.sent[0] == {"chat_id": "-100123", "text": "hi", "message_thread_id": 42}
    assert fake_tg.sent[1] == {"chat_id": "5551234", "text": "dm"}
    assert fake_tg.paths[0] == f"/bot{TOKEN}/sendMessage"


async def test_bot_token_is_never_logged(notifier, fake_tg, caplog, log, clock, db):
    caplog.set_level(logging.DEBUG)
    fake_tg.fail = [httpx.ConnectError(f"failed https://api.telegram.org/bot{TOKEN}/x"), 500]
    await notifier.send(out(text="A"))
    await notifier.drain_once()
    clock.advance(timedelta(minutes=1))
    await notifier.drain_once()
    clock.advance(timedelta(minutes=2))
    await notifier.drain_once()
    assert fake_tg.texts() == ["A"]
    assert TOKEN not in caplog.text
    rows = db_text(db)
    assert TOKEN not in rows
    assert all(TOKEN not in str(e["payload"]) for e in log.query(kinds=["notify.retry"]))


def db_text(db: Database) -> str:
    return str([tuple(r) for r in db.execute("SELECT * FROM outbox").fetchall()])


# --- background loop -----------------------------------------------------------------


async def test_drain_loop_delivers_and_stops(notifier, fake_tg):
    stop = asyncio.Event()
    task = asyncio.create_task(notifier.drain(stop))
    await notifier.send(out(text="A"))
    for _ in range(100):
        if fake_tg.sent:
            break
        await asyncio.sleep(0.01)
    assert fake_tg.texts() == ["A"]
    stop.set()
    await asyncio.wait_for(task, 2)


async def test_next_due_none_when_empty(notifier, clock):
    assert notifier.next_due() is None
    assert DAY == clock.now()
