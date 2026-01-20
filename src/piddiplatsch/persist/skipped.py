import logging
from datetime import UTC, datetime
from pathlib import Path

from piddiplatsch.config import config
from piddiplatsch.persist.base import JsonlRecorder


class SkipRecorder:
    SKIPPED_DIR = (
        Path(config.get("consumer", {}).get("output_dir", "outputs")) / "skipped"
    )

    @staticmethod
    def record_skipped_item(key: str, data: dict, reason: str | None = None) -> None:
        """Append a skipped item to daily JSONL with timestamp and reason."""
        timestamp = datetime.now(UTC).isoformat(timespec="seconds")
        infos = {
            "skip_timestamp": timestamp,
            "retries": int(data.get("retries", 0)),
            "reason": reason or "Unknown",
        }
        try:
            recorder = JsonlRecorder(SkipRecorder.SKIPPED_DIR, "skipped_items")
            path = recorder.record(data, infos=infos)
            logging.warning(f"Recorded skipped item {key} to {path}")
        except Exception:
            logging.exception(f"Failed to record skipped item {key}")
