import logging
from pathlib import Path

from piddiplatsch.config import config
from piddiplatsch.persist.base import JsonlRecorder


class DumpRecorder:
    DUMP_DIR = Path(config.get("consumer", {}).get("output_dir", "outputs")) / "dump"
    DUMP_DIR.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def record_item(key: str, data: dict) -> None:
        # Write raw message as-is, one JSON per line
        recorder = JsonlRecorder(DumpRecorder.DUMP_DIR, "dump_messages")
        path = recorder.record(data)
        logging.debug(f"Dumped message {key} to {path}")
