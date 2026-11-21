import json
from datetime import datetime

from swarm.engine.base import QUOTA_ERROR_PREFIX, Error, quota_error
from swarm.workers.contract import (
    STUCK_REASON,
    cancelled_summary,
    is_quota_pause,
    order_from_json,
    paused_summary,
)

from .conftest import order


def test_cancelled_summary_and_quota_pause_match_pool_strings():
    assert cancelled_summary("stuck") == "cancelled: stuck"
    assert cancelled_summary(STUCK_REASON) == "cancelled: stuck"
    assert is_quota_pause("paused: quota:limit_hit")
    assert not is_quota_pause("paused: x")


def test_quota_error_is_what_a_quota_pause_summary_is_made_from():
    err = quota_error("limit_hit")
    assert err == Error(message="quota:limit_hit")
    assert err.message.startswith(QUOTA_ERROR_PREFIX)
    assert is_quota_pause(paused_summary(err.message))
    assert paused_summary("quota:limit_hit") == "paused: quota:limit_hit"


def test_order_from_json_parses_due():
    raw = json.dumps(order(due="2026-05-01T09:00:00"))
    assert order_from_json(raw)["due"] == datetime(2026, 5, 1, 9, 0)
    assert order_from_json(json.dumps(order()))["due"] is None
