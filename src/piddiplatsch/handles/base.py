from typing import Any, Protocol


# --- Backend Interface ---
class HandleBackend(Protocol):
    """Minimal interface for PID backends, aligned with Handle API."""

    def add(self, pid: str, record: dict[str, Any]) -> None: ...

    def get(self, pid: str) -> dict[str, Any] | None: ...

    def update(self, pid: str, record: dict[str, Any]) -> None: ...
