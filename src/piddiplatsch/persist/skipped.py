import logging
from datetime import UTC, datetime
from pathlib import Path

from piddiplatsch.config import config
from piddiplatsch.persist.base import RecorderBase


class SkipRecorder(RecorderBase):
    SKIPPED_DIR = (
        Path(config.get("consumer", {}).get("output_dir", "outputs")) / "skipped"
    )

    def __init__(self) -> None:
        super().__init__(self.SKIPPED_DIR, "skipped_items")

    def prepare(
        self,
        key: str,
        data: dict,
        reason: str | None,
        retries: int | None,
    ) -> tuple[dict, dict | None, Path | None]:
        timestamp = datetime.now(UTC).isoformat(timespec="seconds")
        payload = dict(data)
        # Determine retries value: explicit overrides payload value
        payload_retries = retries
        if payload_retries is None:
            payload_retries = int(
                (payload.get("__infos__", {}) or {}).get(
                    "retries", payload.get("retries", 0)
                )
            )
        else:
            # reflect retries in payload for loader semantics
            if "__infos__" in payload:
                payload["__infos__"]["retries"] = payload_retries
            else:
                payload["retries"] = payload_retries

        infos = {
            "skip_timestamp": timestamp,
            "retries": int(payload_retries or 0),
            "reason": reason or "Unknown",
        }
        return payload, infos, None

    @staticmethod
    def record(
        key: str, data: dict, *, reason: str | None = None, retries: int | None = None
    ) -> None:
        try:
            path = SkipRecorder().write(key, data, reason=reason, retries=retries)
            logging.warning(f"Recorded skipped item {key} to {path}")
        except Exception:
            logging.exception(f"Failed to record skipped item {key}")
