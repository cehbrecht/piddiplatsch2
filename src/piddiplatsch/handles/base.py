from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from piddiplatsch.utils.models import build_handle, prepare_handle_data


class HandleBackend(ABC):
    """
    Abstract backend template. Implements create-or-update logic.
    """

    def add(self, pid: str, record: dict[str, Any]) -> None:
        handle, handle_data = self._prepare(pid, record)
        self._store(handle, handle_data)

    def _prepare(self, pid: str, record: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        handle = build_handle(pid)
        handle_data = prepare_handle_data(record)

        if "URL" not in handle_data or not handle_data["URL"]:
            raise ValueError("Missing required 'URL' in record")

        return handle, handle_data

    @abstractmethod
    def _store(self, handle: str, handle_data: dict[str, Any]) -> None:
        raise NotImplementedError


