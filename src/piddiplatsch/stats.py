import logging
import json
from collections import Counter
from datetime import datetime
from piddiplatsch.config import config


class StatsTracker:
    def __init__(self):
        self.datasets_processed = 0
        self.file_handles_created = 0
        self.failures = 0
        self.counter = Counter()
        self.logger = logging.getLogger(__name__)
        self.summary_interval = config.get("consumer", {}).get(
            "stats_summary_interval", 100
        )

    def _log_json(self, level, event_type, data):
        log_record = {
            "event": event_type,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            **data,
        }
        log_fn = getattr(self.logger, level)
        log_fn(json.dumps(log_record))

    def record_success(self, dataset_id, num_files, elapsed=None):
        self.datasets_processed += 1
        self.file_handles_created += num_files
        self.counter["success"] += 1

        self._log_json(
            "info",
            "dataset_success",
            {
                "dataset_id": dataset_id,
                "files": num_files,
                "elapsed_sec": round(elapsed, 3) if elapsed else None,
            },
        )

        if (
            self.summary_interval
            and self.datasets_processed % self.summary_interval == 0
        ):
            self.log_summary()

    def record_failure(self, dataset_id, error):
        self.failures += 1
        self.counter["failure"] += 1

        self._log_json(
            "error", "dataset_failure", {"dataset_id": dataset_id, "error": str(error)}
        )

    def summary(self):
        return {
            "datasets_processed": self.datasets_processed,
            "file_handles_created": self.file_handles_created,
            "failures": self.failures,
            "counters": dict(self.counter),
        }

    def log_summary(self):
        self._log_json("info", "processing_summary", self.summary())
