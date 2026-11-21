import json
from datetime import date, datetime

import pytest

from swarm.clock import SEOUL
from swarm.store.codec import to_json


def test_to_json_matches_isoformat_default():
    x = {"due": datetime(2026, 11, 6, 9, 0, tzinfo=SEOUL), "n": 1, "t": "한"}
    assert to_json(x) == json.dumps(x, ensure_ascii=False, default=lambda o: o.isoformat())


def test_to_json_encodes_dates():
    assert to_json({"d": date(2026, 11, 6)}) == '{"d": "2026-11-06"}'


def test_to_json_raises_on_an_unserializable_value_like_json_dumps():
    with pytest.raises(TypeError):
        to_json({"o": {1}})
