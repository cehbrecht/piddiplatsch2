import datetime
import logging
import sqlite3
import time
from enum import Enum

logger = logging.getLogger(__name__)


# -----------------------
# Enum for counters
# -----------------------
class CounterKey(str, Enum):
    MESSAGES = "messages"
    ERRORS = "errors"
    RETRIES = "retries"
    HANDLES = "handles"
    RETRACTED = "retracted_messages"
    REPLICAS = "replicas"
    WARNINGS = "warnings"


# -----------------------
# Reporter base
# -----------------------
class StatsReporter:
    """Base class for stats reporters."""

    def log(self, summary: dict):
        raise NotImplementedError

    def close(self):
        """Optional cleanup hook (no-op by default)."""
        return None


class ConsoleReporter(StatsReporter):
    def log(self, summary: dict):
        logger.info(f"Stats snapshot: {summary}")


class SQLiteReporter(StatsReporter):
    def __init__(self, db_path: str = "stats.db"):
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._cursor = self._conn.cursor()
        self._cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS message_stats (
                ts TIMESTAMP PRIMARY KEY,
                messages INTEGER,
                errors INTEGER,
                retries INTEGER,
                handles INTEGER,
                retracted_messages INTEGER,
                replicas INTEGER,
                warnings INTEGER,
                uptime REAL
            )
            """
        )
        self._conn.commit()
        self._closed = False

    def log(self, summary: dict):
        if self._closed:
            raise RuntimeError("SQLiteReporter is closed")

        ts = datetime.datetime.utcnow()
        self._cursor.execute(
            """
            INSERT INTO message_stats (ts, messages, errors, retries, handles,
                                       retracted_messages, replicas, warnings, uptime)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ts,
                summary[CounterKey.MESSAGES.value],
                summary[CounterKey.ERRORS.value],
                summary[CounterKey.RETRIES.value],
                summary[CounterKey.HANDLES.value],
                summary[CounterKey.RETRACTED.value],
                summary[CounterKey.REPLICAS.value],
                summary[CounterKey.WARNINGS.value],
                summary["uptime"],
            ),
        )
        self._conn.commit()

    def close(self):
        if getattr(self, "_closed", False):
            return
        try:
            # attempt to flush/close cleanly
            try:
                self._cursor.close()
            except Exception:
                pass
            try:
                self._conn.close()
            except Exception:
                pass
        finally:
            self._closed = True


# -----------------------
# Stats singleton
# -----------------------
class Stats:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(
        self,
        log_interval_seconds: int = 10,
        log_interval_messages: int = 100,
        db_path: str | None = None,
    ):
        if getattr(self, "_initialized", False):
            return

        # Private timestamps
        self._start_time = time.time()
        self._last_message_time = None
        self._last_error_time = None

        # Initialize all counters to 0
        self._counters = dict.fromkeys(CounterKey, 0)

        # Logging control
        self.log_interval_seconds = log_interval_seconds
        self.log_interval_messages = log_interval_messages
        self._last_log_time = time.time()
        self._last_logged_messages = 0

        # Reporters: always console, optionally SQLite
        self.reporters: list[StatsReporter] = [ConsoleReporter()]
        if db_path:
            self.reporters.append(SQLiteReporter(db_path=db_path))

        # closed flag for cleanup
        self._closed = False
        self._initialized = True

    # --- Core counter increment ---
    def increment(self, key: CounterKey, n: int = 1):
        """Increment any counter dynamically (pure counter, no side effects)."""
        self._counters[key] += n
        if key == CounterKey.MESSAGES:
            self._last_message_time = time.time()
            self._maybe_log()

    # --- Shortcuts (increment + optional logging + timestamps) ---
    def tick(self, n: int = 1):
        self.increment(CounterKey.MESSAGES, n)

    def retry(self, n: int = 1):
        self.increment(CounterKey.RETRIES, n)

    def handle(self, n: int = 1):
        self.increment(CounterKey.HANDLES, n)

    def error(self, message: str | None = None, n: int = 1):
        self.increment(CounterKey.ERRORS, n)
        self._last_error_time = time.time()
        if message:
            logger.error(f"ERROR: {message}")

    def retracted(self, message: str | None = None, n: int = 1):
        self.increment(CounterKey.RETRACTED, n)
        if message:
            logger.warning(f"RETRACTED: {message}")

    def replica(self, message: str | None = None, n: int = 1):
        self.increment(CounterKey.REPLICAS, n)
        if message:
            logger.info(f"REPLICA: {message}")

    def warn(self, message: str | None = None, n: int = 1):
        self.increment(CounterKey.WARNINGS, n)
        if message:
            logger.warning(f"WARNING: {message}")

    # --- Internal logging / persistence ---
    def _maybe_log(self):
        now = time.time()
        messages_since_last = (
            self._counters[CounterKey.MESSAGES] - self._last_logged_messages
        )

        if messages_since_last == 0:
            return  # nothing new

        if (now - self._last_log_time >= self.log_interval_seconds) or (
            messages_since_last >= self.log_interval_messages
        ):
            self._log_stats()
            self._last_log_time = now
            self._last_logged_messages = self._counters[CounterKey.MESSAGES]

    def _log_stats(self):
        summary = self.summary()
        for reporter in list(self.reporters):
            try:
                reporter.log(summary)
            except Exception:
                logger.exception("Failed to log stats with reporter %s", reporter)

    # --- Accessors ---
    def __getitem__(self, key: CounterKey):
        return self._counters[key]

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
        summary = {key.value: self._counters[key] for key in CounterKey}
        summary.update(
            {
                "uptime": time.time() - self.start_time,
                "last_message_time": self.last_message_time,
                "last_error_time": self.last_error_time,
            }
        )
        return summary

    # --- Cleanup ---
    def close(self):
        """Close all reporters (idempotent)."""
        if getattr(self, "_closed", False):
            return
        for reporter in list(self.reporters):
            try:
                reporter.close()
            except Exception:
                logger.exception("Error closing reporter %s", reporter)
        self._closed = True


# Singleton instance (no DB by default)
stats = Stats()

# If you want DB persistence:
# stats = Stats(db_path="stats.db")
