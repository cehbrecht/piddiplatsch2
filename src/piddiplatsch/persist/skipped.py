import logging
from datetime import UTC, datetime
from pathlib import Path

from piddiplatsch.config import config
from piddiplatsch.persist.base import DailyJsonlWriter


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
        payload = DailyJsonlWriter.wrap_with_infos(data, infos)
        try:
            writer = DailyJsonlWriter(SkipRecorder.SKIPPED_DIR)
            path = writer.write("skipped_items", payload)
            logging.warning(f"Recorded skipped item {key} to {path}")
        except Exception:
            logging.exception(f"Failed to record skipped item {key}")
