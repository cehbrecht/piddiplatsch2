from __future__ import annotations

from typing import Any, Protocol

from piddiplatsch.handles import get_handle_backend
from piddiplatsch.handles.base import HandleBackend


class HandleAPIProtocol(Protocol):
    """Protocol defining the public Handle API for processors."""

    def add(self, pid: str, record: dict[str, Any]) -> None: ...


class HandleAPI(HandleAPIProtocol):
    """User-facing API wrapping a backend."""

    def __init__(self, backend: HandleBackend | None = None):
        self.backend: HandleBackend = backend or get_handle_backend()

    def add(self, pid: str, record: dict[str, Any]) -> None:
        self.backend.add(pid, record)
