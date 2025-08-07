import json
import logging
import time
from datetime import datetime

from piddiplatsch.config import config


class StatsTracker:
    def __init__(self):
        self.messages_processed = 0
        self.handles_created = 0
        self.failures = 0
        self.start_time = time.time()

        self.logger = logging.getLogger(__name__)
        self.summary_interval = config.get("consumer", {}).get(
            "stats_summary_interval", 100
        )

    def _log_json(self, level: str, event: str, data: dict):
        log_record = {
            "event": event,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            **data,
        }
        log_fn = getattr(self.logger, level)
        log_fn(json.dumps(log_record))

    def record_success(self, key: str, num_handles: int, elapsed: float):
        self.messages_processed += 1
        self.handles_created += num_handles

        self._log_json(
            "info",
            "success",
            {
                "key": key,
                "handles": num_handles,
                "elapsed_sec": round(elapsed, 3) if elapsed else None,
            },
        )

        if (
            self.summary_interval
            and self.messages_processed % self.summary_interval == 0
        ):
            self.log_summary()

    def record_failure(self, key: str, error: str):
        self.failures += 1

        self._log_json(
            "error",
            "failure",
            {
                "key": key,
                "error": str(error),
            },
        )

    def summary(self):
        elapsed = time.time() - self.start_time
        return {
            "messages_processed": self.messages_processed,
            "handles_created": self.handles_created,
            "failures": self.failures,
            "elapsed_sec": round(elapsed, 1),
            "messages_per_min": (
                round((self.messages_processed / elapsed) * 60, 2) if elapsed > 0 else 0
            ),
            "handles_per_min": (
                round((self.handles_created / elapsed) * 60, 2) if elapsed > 0 else 0
            ),
        }

    def log_summary(self):
        self._log_json("info", "processing_summary", self.summary())
