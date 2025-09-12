import logging
import time

logger = logging.getLogger(__name__)


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
        # Timestamps for messages/errors
        self.start_time = time.time()
        self.last_message_time = None
        self.last_error_time = None

        # All counters
        self._counters = {}
        self._initialized = True

    # --- Core counter increment ---
    def increment(self, key, n=1):
        """Increment any counter dynamically (pure counter, no side effects)."""
        self._counters[key] = self._counters.get(key, 0) + n

    # --- Shortcuts (increment + optional logging + timestamps) ---
    def tick(self, n=1):
        """Increment messages counter and update timestamp."""
        self.increment("messages", n)
        self.last_message_time = time.time()

    def retry(self, n=1):
        """Increment retries counter."""
        self.increment("retries", n)

    def handle(self, n=1):
        """Increment handle counter."""
        self.increment("handles", n)

    def error(self, message=None, n=1):
        """Increment errors counter, update timestamp, and log a message if provided."""
        self.increment("errors", n)
        self.last_error_time = time.time()
        if message:
            logger.error(f"ERROR: {message}")

    def retracted(self, message=None, n=1):
        """Increment retracted messages counter and log a warning message if provided."""
        self.increment("retracted_messages", n)
        if message:
            logger.warning(f"RETRACTED: {message}")

    def warn(self, message=None, n=1):
        """Increment warnings counter and log a warning message."""
        self.increment("warnings", n)
        if message:
            logger.warning(f"WARNING: {message}")

    # --- access counters ---
    @property
    def errors(self):
        return self._counters.get("errors", 0)

    @property
    def messages(self):
        return self._counters.get("messages", 0)

    # --- Summary ---
    def summary(self):
        """Return all counters and timestamps as a dict."""
        summary = dict(self._counters)
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
