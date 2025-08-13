import json
import logging
from datetime import datetime, timezone

from piddiplatsch.config import config
from piddiplatsch.processing import ProcessingResult


class MetricsTracker:
    def __init__(self):
        self.messages_processed = 0
        self.handles_created = 0
        self.failures = 0
        self.total_schema_validation_time = 0.0  # seconds
        self.total_record_validation_time = 0.0  # seconds
        self.total_handle_processing_time = 0.0  # seconds
        self.start_time = datetime.now(timezone.utc)
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
        getattr(self.logger, level)(json.dumps(log_record))

    def record_result(self, result: ProcessingResult):
        if result.success:
            self.messages_processed += 1
            self.handles_created += result.num_handles
            # Aggregate timings if available
            self.total_schema_validation_time += getattr(
                result, "schema_validation_time", 0.0
            )
            self.total_record_validation_time += getattr(
                result, "record_validation_time", 0.0
            )
            self.total_handle_processing_time += getattr(
                result, "handle_processing_time", 0.0
            )
            self._log_json("info", "success", result.__dict__)
        else:
            self.failures += 1
            self._log_json("error", "failure", result.__dict__)

        if (
            result.success
            and self.summary_interval
            and self.messages_processed % self.summary_interval == 0
        ):
            self.log_summary()

    def summary(self):
        elapsed = (datetime.now(timezone.utc) - self.start_time).total_seconds() or 1
        return {
            "messages_processed": self.messages_processed,
            "handles_created": self.handles_created,
            "failures": self.failures,
            "elapsed_sec": round(elapsed, 1),
            "messages_per_sec": round(self.messages_processed / elapsed, 2),
            "handles_per_sec": round(self.handles_created / elapsed, 2),
            "total_schema_validation_time_sec": round(
                self.total_schema_validation_time, 2
            ),
            "total_record_validation_time_sec": round(
                self.total_record_validation_time, 2
            ),
            "total_handle_time_sec": round(self.total_handle_processing_time, 2),
            "avg_validation_time_per_message": round(
                (self.total_schema_validation_time + self.total_record_validation_time)
                / max(self.messages_processed, 1),
                4,
            ),
            "avg_handle_time_per_message": round(
                self.total_handle_processing_time / max(self.messages_processed, 1), 4
            ),
        }

    def log_summary(self):
        self._log_json("info", "processing_summary", self.summary())
