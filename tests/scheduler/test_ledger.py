from datetime import timedelta

import pytest

from swarm.scheduler.ledger import SqliteQuotaLedger
from swarm.types import UsageSignal


def sig(clock, kind, reset=None):
    return UsageSignal(kind=kind, reset_at=reset, at=clock.now())


def kinds(log):
    return [e["kind"] for e in log.query()]


def test_limit_hit_cools_until_reset(ledger, clock):
    reset = clock.now() + timedelta(hours=2)
    ledger.observe(engine="claude", signal=sig(clock, "limit_hit", reset))
    assert ledger.lease(engine_pref=["claude"], purpose="respond") is None
    assert ledger.next_reset("claude") == reset
    clock.advance(timedelta(hours=2, seconds=1))
    assert ledger.lease(engine_pref=["claude"], purpose="respond") is not None


def test_limit_without_reset_time_cools_one_hour(ledger, clock):
    ledger.observe(engine="claude", signal=sig(clock, "limit_hit"))
    assert ledger.next_reset("claude") == clock.now() + timedelta(hours=1)
    clock.advance(timedelta(minutes=59))
    assert ledger.lease(engine_pref=["claude"], purpose="respond") is None
    clock.advance(timedelta(minutes=2))
    assert ledger.lease(engine_pref=["claude"], purpose="respond") is not None


def test_parallel_cap(ledger):
    leases = [ledger.lease(engine_pref=["claude"], purpose="advance") for _ in range(4)]
    assert sum(lease is not None for lease in leases) == 3


def test_auth_failed_disables_until_auth_ok(ledger, clock, log):
    ledger.observe(engine="claude", signal=sig(clock, "auth_failed"))
    assert ledger.lease(engine_pref=["claude"], purpose="respond") is None
    ledger.observe(engine="claude", signal=sig(clock, "auth_ok"))
    assert ledger.lease(engine_pref=["claude"], purpose="respond") is not None
    assert {"engine.auth_failed", "engine.auth_ok"} <= set(kinds(log))


def test_release_frees_slot(ledger, log):
    ls = [ledger.lease(engine_pref=["claude"], purpose="advance") for _ in range(3)]
    assert ledger.lease(engine_pref=["claude"], purpose="advance") is None
    ledger.release(ls[0])
    assert ledger.lease(engine_pref=["claude"], purpose="advance") is not None
    ledger.release(ls[0])  # double release is a no-op
    assert "scheduler.release" in kinds(log) and "scheduler.lease" in kinds(log)


def test_expired_leases_are_reclaimed(ledger, clock, log):
    for _ in range(3):
        ledger.lease(engine_pref=["claude"], purpose="advance", max_minutes=5)
    assert ledger.lease(engine_pref=["claude"], purpose="advance") is None
    clock.advance(timedelta(minutes=15, seconds=1))
    assert ledger.lease(engine_pref=["claude"], purpose="advance") is not None
    assert kinds(log).count("scheduler.lease_expired") == 3


def test_lease_expiry_is_max_minutes_plus_ten(ledger, clock):
    lease = ledger.lease(engine_pref=["claude"], purpose="advance", max_minutes=30)
    assert lease["expires_at"] == clock.now() + timedelta(minutes=40)
    default = ledger.lease(engine_pref=["claude"], purpose="advance")
    assert default["expires_at"] == clock.now() + timedelta(minutes=70)


def test_empty_engine_pref_raises(ledger):
    with pytest.raises(ValueError):
        ledger.lease(engine_pref=[], purpose="respond")


def test_falls_through_pref_order(db, log, clock):
    led = SqliteQuotaLedger(db, log, clock, "i", caps={"claude": 1, "codex": 1})
    a = led.lease(engine_pref=["claude", "codex"], purpose="advance")
    b = led.lease(engine_pref=["claude", "codex"], purpose="advance")
    assert (a["engine"], b["engine"]) == ("claude", "codex")
    assert led.lease(engine_pref=["claude", "codex"], purpose="advance") is None


def test_unknown_engine_is_not_leased(ledger):
    assert ledger.lease(engine_pref=["codex"], purpose="respond") is None


def test_next_reset_none_and_default(ledger, clock):
    assert ledger.next_reset() == clock.now()
    assert ledger.next_reset("claude") == clock.now()
    r1 = clock.now() + timedelta(hours=3)
    ledger.observe(engine="claude", signal=sig(clock, "limit_hit", r1))
    assert ledger.next_reset() == r1


def test_limit_hit_does_not_shorten_longer_cooling(ledger, clock):
    far = clock.now() + timedelta(hours=5)
    ledger.observe(engine="claude", signal=sig(clock, "limit_hit", far))
    ledger.observe(
        engine="claude", signal=sig(clock, "limit_hit", clock.now() + timedelta(hours=1))
    )
    assert ledger.next_reset("claude") == far


def test_state_survives_restart_and_stale_leases_released(db, log, clock):
    a = SqliteQuotaLedger(db, log, clock, "inst_a")
    for _ in range(3):
        a.lease(engine_pref=["claude"], purpose="advance")
    reset = clock.now() + timedelta(hours=2)
    a.observe(engine="claude", signal=sig(clock, "limit_hit", reset))
    b = SqliteQuotaLedger(db, log, clock, "inst_b")
    assert b.next_reset("claude") == reset
    clock.advance(timedelta(hours=2, seconds=1))
    assert b.lease(engine_pref=["claude"], purpose="advance") is not None  # stale leases gone
    c = SqliteQuotaLedger(db, log, clock, "inst_b")  # same instance: leases kept
    assert len([c.lease(engine_pref=["claude"], purpose="advance") for _ in range(3)]) == 3
    assert c.lease(engine_pref=["claude"], purpose="advance") is None


def test_disabled_survives_restart(db, log, clock):
    a = SqliteQuotaLedger(db, log, clock, "inst_a")
    a.observe(engine="claude", signal=sig(clock, "auth_failed"))
    b = SqliteQuotaLedger(db, log, clock, "inst_b")
    assert b.lease(engine_pref=["claude"], purpose="respond") is None


def test_disabled_reports_the_auth_state(ledger, clock):
    assert ledger.disabled("claude") is False
    ledger.observe(engine="claude", signal=sig(clock, "auth_failed"))
    assert ledger.disabled("claude") is True
    ledger.observe(engine="claude", signal=sig(clock, "auth_ok"))
    assert ledger.disabled("claude") is False


def test_limit_with_a_reset_time_already_past_still_rests_a_minute(ledger, clock):
    """A stale reset_at must not leave the engine leasable at once (hot retry)."""
    ledger.observe(
        engine="claude", signal=sig(clock, "limit_hit", clock.now() - timedelta(hours=1))
    )
    assert ledger.lease(engine_pref=["claude"], purpose="respond") is None
    clock.advance(timedelta(minutes=1, seconds=1))
    assert ledger.lease(engine_pref=["claude"], purpose="respond") is not None
