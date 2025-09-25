import json
import logging
from datetime import UTC, datetime
from pathlib import Path

from piddiplatsch.config import config


class DumpRecorder:
    DUMP_DIR = Path(config.get("consumer", {}).get("output_dir", "outputs")) / "dump"
    DUMP_DIR.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def record_item(key: str, data: dict) -> None:
        now = datetime.now(UTC)
        dated_filename = f"dump_messages_{now.date()}.jsonl"
        dump_file = DumpRecorder.DUMP_DIR / dated_filename
        # record = {"key": key, "value": data, "timestamp": now.isoformat()}
        record = data
        with dump_file.open("a", encoding="utf-8") as f:
            json.dump(record, f)
            f.write("\n")

        logging.debug(f"Dumped message {key} to {dump_file}")
