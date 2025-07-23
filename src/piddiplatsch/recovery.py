import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from piddiplatsch.config import config


class FailureRecovery:
    FAILURE_DIR = (
        Path(config.get("consumer", {}).get("output_dir", "outputs")) / "failures"
    )
    FAILURE_DIR.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def record_failed_item(key: str, data: dict) -> None:
        """Append a failed STAC item to a daily JSONL file with UTC timestamp."""
        now = datetime.now(timezone.utc)
        timestamp = now.isoformat(timespec="seconds")
        dated_filename = f"failed_items_{now.date()}.jsonl"
        failure_file = FailureRecovery.FAILURE_DIR / dated_filename

        data_with_timestamp = {**data, "failure_timestamp": timestamp}

        with failure_file.open("a", encoding="utf-8") as f:
            json.dump(data_with_timestamp, f)
            f.write("\n")
        logging.warning(f"Recorded failed item {key} to {failure_file}")
