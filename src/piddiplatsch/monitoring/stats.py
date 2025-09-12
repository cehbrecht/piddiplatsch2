import logging
import time
from enum import Enum

logger = logging.getLogger(__name__)


class CounterKey(str, Enum):
    MESSAGES = "messages"
    ERRORS = "errors"
    RETRIES = "retries"
    HANDLES = "handles"
    RETRACTED = "retracted_messages"
    REPLICAS = "replicas"
    WARNINGS = "warnings"


class Stats:
    """Singleton class tracking message stats, errors, retries, handles, retracted messages, warnings, etc."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if getattr(self, "_initialized", False):
            return
        # Private timestamps
        self._start_time = time.time()
        self._last_message_time = None
        self._last_error_time = None

        # Initialize all counters to 0
        self._counters = dict.fromkeys(CounterKey, 0)
        self._initialized = True

    # --- Core counter increment ---
    def increment(self, key: CounterKey, n=1):
        """Increment any counter dynamically (pure counter, no side effects)."""
        self._counters[key] += n

    # --- Shortcuts (increment + optional logging + timestamps) ---
    def tick(self, n=1):
        """Increment messages counter and update timestamp."""
        self.increment(CounterKey.MESSAGES, n)
        self._last_message_time = time.time()

    def retry(self, n=1):
        """Increment retries counter."""
        self.increment(CounterKey.RETRIES, n)

    def handle(self, n=1):
        """Increment handle counter."""
        self.increment(CounterKey.HANDLES, n)

    def error(self, message=None, n=1):
        """Increment errors counter, update timestamp, and log a message if provided."""
        self.increment(CounterKey.ERRORS, n)
        self._last_error_time = time.time()
        if message:
            logger.error(f"ERROR: {message}")

    def retracted(self, message=None, n=1):
        """Increment retracted messages counter and log a warning message if provided."""
        self.increment(CounterKey.RETRACTED, n)
        if message:
            logger.warning(f"RETRACTED: {message}")

    def replica(self, message=None, n=1):
        """Increment replica messages counter and log a info message if provided."""
        self.increment(CounterKey.REPLICAS, n)
        if message:
            logger.info(f"REPLICA: {message}")

    def warn(self, message=None, n=1):
        """Increment warnings counter and log a warning message."""
        self.increment(CounterKey.WARNINGS, n)
        if message:
            logger.warning(f"WARNING: {message}")

    # --- Enum-based internal access ---
    def __getitem__(self, key: CounterKey):
        return self._counters[key]

    # --- Accessors for main counters (keep properties for convenience) ---
    @property
    def messages(self):
        return self._counters[CounterKey.MESSAGES]

    @property
    def replicas(self):
        return self._counters[CounterKey.REPLICAS]

    @property
    def retracted_messages(self):
        return self._counters[CounterKey.RETRACTED]

    @property
    def errors(self):
        return self._counters[CounterKey.ERRORS]

    @property
    def start_time(self):
        return self._start_time

    @property
    def last_message_time(self):
        return self._last_message_time

    @property
    def last_error_time(self):
        return self._last_error_time

    # --- Summary ---
    def summary(self):
        """Return all counters and timestamps as a dict using CounterKey values as keys."""
        summary = {key.value: self._counters[key] for key in CounterKey}
        summary.update(
            {
                "uptime": time.time() - self.start_time,
                "last_message_time": self.last_message_time,
                "last_error_time": self.last_error_time,
            }
        )
        return summary


# Singleton instance
stats = Stats()
