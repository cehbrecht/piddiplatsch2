import logging
from pathlib import Path

from piddiplatsch.config import config
from piddiplatsch.persist.base import RecorderBase
from piddiplatsch.result import PrepareResult


class DumpRecorder(RecorderBase):
    LOG_KIND = "dump"
    LOG_LEVEL = logging.DEBUG
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
    ) -> PrepareResult:
        # No infos, just write raw payload
        return PrepareResult(payload=data)
