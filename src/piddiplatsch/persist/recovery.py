import logging
from datetime import UTC, datetime
from pathlib import Path

from piddiplatsch.config import config
from piddiplatsch.persist.base import RecorderBase
from piddiplatsch.persist.helpers import PrepareResult


class FailureRecorder(RecorderBase):
    LOG_KIND = "failure"
    LOG_LEVEL = logging.WARNING
    FAILURE_DIR = (
        Path(config.get("consumer", {}).get("output_dir", "outputs")) / "failures"
    )
    FAILURE_DIR.mkdir(parents=True, exist_ok=True)

    def __init__(self) -> None:
        super().__init__(self.FAILURE_DIR, "failed_items")

    def prepare(
        self,
        key: str,
        data: dict,
        reason: str | None,
        retries: int | None,
    ) -> PrepareResult:
        ts = datetime.now(UTC).isoformat(timespec="seconds")
        r = 0 if retries is None else int(retries)
        infos = {"failure_timestamp": ts, "retries": r, "reason": reason or "Unknown"}
        subdir = self.root_dir / f"r{r}"
        return PrepareResult(payload=data, infos=infos, subdir=subdir)
