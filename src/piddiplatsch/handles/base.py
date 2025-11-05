from pathlib import Path
from typing import Any, Literal, Protocol

from piddiplatsch.config import config

from .jsonl_backend import JsonlHandleBackend
from .pyhandle_backend import HandleClient


# --- Backend Interface ---
class HandleBackend(Protocol):
    """Minimal interface for PID backends, aligned with Handle API."""

    def add(self, pid: str, record: dict[str, Any]) -> None: ...

    def get(self, pid: str) -> dict[str, Any] | None: ...

    def update(self, pid: str, record: dict[str, Any]) -> None: ...


# --- Factory Function ---
def get_handle_backend() -> HandleBackend:
    """
    Return a HandleBackend based on configuration.

    Config keys expected in [handle] section:
      backend = "pyhandle" | "jsonl"
      jsonl_path = "test-handles.jsonl"  # only for jsonl
    """
    backend_type: Literal["pyhandle", "jsonl"] = config.get(
        "handle", "backend", fallback="pyhandle"
    )

    if backend_type == "pyhandle":
        return HandleClient.from_config()

    if backend_type == "jsonl":
        path = config.get("handle", "jsonl_path", fallback="handles.jsonl")
        return JsonlHandleBackend(Path(path))

    raise ValueError(f"Unknown handle backend type: {backend_type}")
