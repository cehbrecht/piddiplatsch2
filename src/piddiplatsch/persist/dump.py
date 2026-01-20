import logging
from pathlib import Path

from piddiplatsch.config import config
from piddiplatsch.persist.base import DailyJsonlWriter


class DumpRecorder:
    DUMP_DIR = Path(config.get("consumer", {}).get("output_dir", "outputs")) / "dump"
    DUMP_DIR.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def record_item(key: str, data: dict) -> None:
        # Write raw message as-is, one JSON per line
        writer = DailyJsonlWriter(DumpRecorder.DUMP_DIR)
        path = writer.write("dump_messages", data)
        logging.debug(f"Dumped message {key} to {path}")
