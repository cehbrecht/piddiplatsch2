from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from piddiplatsch.config import config
from piddiplatsch.handles.base import HandleBackend


class JsonlHandleBackend(HandleBackend):
    """
    JSONL backend for testing, similar to DumpRecorder.
    Writes one file per day under the configured dump directory.
    Format per line:
      {"handle": "<HANDLE>", "URL": "<location>", "data": {...}, "timestamp": "<ISO8601>"}
    """

    def __init__(self):
        # Read output_dir from [consumer] section, fallback to outputs/handles_dump
        base_dir = config.get("consumer", {}).get("output_dir", "outputs")
        dump_dir = Path(base_dir) / "handles"

        self.dump_dir = Path(dump_dir)
        self.dump_dir.mkdir(parents=True, exist_ok=True)

    def _store(self, handle: str, handle_data: dict[str, Any]) -> None:
        # Determine file name by UTC date
        now = datetime.now(UTC)
        dated_filename = f"handles_{now.date()}.jsonl"
        dump_file = self.dump_dir / dated_filename

        # Include timestamp for traceability
        record = {
            "handle": handle,
            "URL": handle_data.pop("URL", None),
            "data": handle_data,
            "timestamp": now.isoformat(),
        }

        with dump_file.open("a", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False)
            f.write("\n")

        logging.debug(f"Wrote handle {handle} to {dump_file}")
