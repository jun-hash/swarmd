"""The one JSON encoding for rows the daemon stores and later reads back."""

import json
from datetime import date, datetime
from typing import Any


def _encode(o: Any) -> str:
    if isinstance(o, datetime | date):
        return o.isoformat()
    raise TypeError(f"{type(o).__name__} is not JSON serializable")


def to_json(obj: Any) -> str:
    """Datetimes and dates become ISO-8601 text; any other unknown type raises TypeError."""
    return json.dumps(obj, ensure_ascii=False, default=_encode)
