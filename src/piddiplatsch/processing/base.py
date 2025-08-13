import logging
import time
from typing import Any

from piddiplatsch.handle_client import HandleClient
from piddiplatsch.processing.result import ProcessingResult


class BaseProcessor:
    """Base class for STAC record processors with timing and handle support."""

    def __init__(
        self,
        handle_client: HandleClient | None = None,
        retries: int = 0,
    ):
        self.handle_client = handle_client or HandleClient.from_config()
        self.retries = retries
        self.logger = logging.getLogger(__name__)

    def _safe_add_record(self, record) -> None:
        last_err = None
        for attempt in range(1, self.retries + 2):
            try:
                self.handle_client.add_record(record.pid, record.as_record())
                return
            except Exception as e:
                last_err = e
                self.logger.warning(
                    f"Attempt {attempt} failed to add PID {record.pid}: {e}"
                )
        raise RuntimeError(
            f"Failed to register handle {record.pid} after {self.retries + 1} attempts"
        ) from last_err

    def _time_function(self, func, *args, **kwargs) -> tuple[Any, float]:
        t0 = time.perf_counter()
        result = func(*args, **kwargs)
        elapsed = time.perf_counter() - t0
        return result, elapsed

    def process(self, key: str, value: dict[str, Any]) -> ProcessingResult:
        """Child classes should implement _process_item and return (num_handles, schema_time, record_time, handle_time)."""
        raise NotImplementedError
