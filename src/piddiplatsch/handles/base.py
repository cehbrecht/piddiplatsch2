import json
from datetime import datetime
from typing import Any, Protocol


def prepare_handle_data(record: dict[str, Any]) -> dict[str, str]:
    """Prepare handle record fields: serialize list/dict values, skip None, convert datetime."""
    prepared: dict[str, str] = {}

    for key, value in record.items():
        if value is None:
            continue

        if isinstance(value, list | dict):

            def serialize(obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                return obj

            value = json.dumps(value, default=serialize)
        elif isinstance(value, datetime):
            value = value.isoformat()

        prepared[key] = value

    return prepared


# --- Backend Interface ---
class HandleBackend(Protocol):
    """Minimal interface for PID backends, aligned with Handle API."""

    def add(self, pid: str, record: dict[str, Any]) -> None: ...

    def get(self, pid: str) -> dict[str, Any] | None: ...

    def update(self, pid: str, record: dict[str, Any]) -> None: ...
