from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from piddiplatsch.config import config
from piddiplatsch.handles.base import HandleBackend
from piddiplatsch.helpers import DailyJsonlWriter, utc_now


class JsonlHandleBackend(HandleBackend):
    """
    JSONL backend for testing, similar to DumpRecorder.
    Writes one file per day under the configured dump directory.
    Format per line:
      {"handle": "<HANDLE>", "URL": "<location>", "data": {...}, "timestamp": "<ISO8601>"}
    """

    def __init__(self):
        # Read output_dir from [consumer] section, fallback to outputs
        base_dir = config.get("consumer", {}).get("output_dir", "outputs")
        dump_dir = Path(base_dir) / "handles"
        # Use shared JSONL writer for daily rotation and append semantics
        self.writer = DailyJsonlWriter(Path(dump_dir))

    def _store(self, handle: str, handle_data: dict[str, Any]) -> None:
        # Include timestamp for traceability; let writer choose the dated filename
        record = {
            "handle": handle,
            "URL": handle_data.pop("URL", None),
            "data": handle_data,
            "timestamp": utc_now().isoformat(),
        }
        path = self.writer.write("handles", record)
        logging.debug(f"Wrote handle {handle} to {path}")

    def _retrieve(self, handle: str) -> dict[str, Any] | None:
        """Retrieval is intentionally not supported for JSONL backend.

        Returning None avoids slow file scans during tests and runtime.
        """
        logging.debug("JSONL handle backend does not support retrieval; returning None")
        return None
