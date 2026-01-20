import json
import logging
from datetime import UTC, datetime
from pathlib import Path

from piddiplatsch.config import config


class SkipRecorder:
    SKIPPED_DIR = (
        Path(config.get("consumer", {}).get("output_dir", "outputs")) / "skipped"
    )
    SKIPPED_DIR.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def record_skipped_item(key: str, data: dict, reason: str | None = None) -> None:
        """Append a skipped item to a daily JSONL file with UTC timestamp.

        Records minimal metadata under `__infos__` including timestamp, reason, and retries counter.
        """
        now = datetime.now(UTC)
        timestamp = now.isoformat(timespec="seconds")
        dated_filename = f"skipped_items_{now.date()}.jsonl"

        skipped_file = SkipRecorder.SKIPPED_DIR / dated_filename

        infos = {
            "skip_timestamp": timestamp,
            "retries": int(data.get("retries", 0)),
            "reason": reason or "Unknown",
        }

        # Store original data with metadata for later retry
        data_with_metadata = {
            **data,
            "__infos__": infos,
        }

        try:
            with skipped_file.open("a", encoding="utf-8") as f:
                json.dump(data_with_metadata, f)
                f.write("\n")
            logging.warning(f"Recorded skipped item {key} to {skipped_file}")
        except Exception:
            logging.exception(f"Failed to record skipped item {key} to {skipped_file}")
