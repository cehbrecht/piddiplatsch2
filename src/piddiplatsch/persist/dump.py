import logging
from pathlib import Path

from piddiplatsch.config import config
from piddiplatsch.persist.base import RecorderBase


class DumpRecorder(RecorderBase):
    DUMP_DIR = Path(config.get("consumer", {}).get("output_dir", "outputs")) / "dump"
    DUMP_DIR.mkdir(parents=True, exist_ok=True)

    def __init__(self) -> None:
        super().__init__(self.DUMP_DIR, "dump_messages")

    def prepare(
        self,
        key: str,
        data: dict,
        reason: str | None,
        retries: int | None,
    ) -> tuple[dict, dict | None, Path | None]:
        # No infos, just write raw payload
        return data, None, None

    @staticmethod
    def record(
        key: str, data: dict, *, reason: str | None = None, retries: int | None = None
    ) -> None:
        path = DumpRecorder().write(key, data, reason=reason, retries=retries)
        logging.debug(f"Dumped message {key} to {path}")
