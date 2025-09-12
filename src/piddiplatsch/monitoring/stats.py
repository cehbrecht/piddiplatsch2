import datetime
import logging
import sqlite3
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
    """Singleton class tracking message stats with optional SQLite persistence."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(
        self, log_interval_seconds=10, log_interval_messages=100, db_path="stats.db"
    ):
        if getattr(self, "_initialized", False):
            return

        # Private timestamps
        self._start_time = time.time()
        self._last_message_time = None
        self._last_error_time = None

        # Initialize all counters
        self._counters = dict.fromkeys(CounterKey, 0)

        # Logging/persistence control
        self.log_interval_seconds = log_interval_seconds
        self.log_interval_messages = log_interval_messages
        self._last_log_time = time.time()
        self._last_logged_messages = 0

        # SQLite setup
        self.db_path = db_path
        self._init_db()

        self._initialized = True

    # --- SQLite setup ---
    def _init_db(self):
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
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

    # --- Core increment ---
    def increment(self, key: CounterKey, n=1):
        self._counters[key] += n
        if key == CounterKey.MESSAGES:
            self._last_message_time = time.time()
            self._maybe_log()

    # --- Shortcuts ---
    def tick(self, n=1):
        self.increment(CounterKey.MESSAGES, n)

    def retry(self, n=1):
        self.increment(CounterKey.RETRIES, n)

    def handle(self, n=1):
        self.increment(CounterKey.HANDLES, n)

    def error(self, message=None, n=1):
        self.increment(CounterKey.ERRORS, n)
        self._last_error_time = time.time()
        if message:
            logger.error(f"ERROR: {message}")

    def retracted(self, message=None, n=1):
        self.increment(CounterKey.RETRACTED, n)
        if message:
            logger.warning(f"RETRACTED: {message}")

    def replica(self, message=None, n=1):
        self.increment(CounterKey.REPLICAS, n)
        if message:
            logger.info(f"REPLICA: {message}")

    def warn(self, message=None, n=1):
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
        s = self.summary()
        ts = datetime.datetime.utcnow()
        # Log to console/file
        logger.info(f"Stats snapshot: {s}")

        # Persist to SQLite
        self._cursor.execute(
            """
            INSERT INTO message_stats
            (ts, messages, errors, retries, handles, retracted_messages, replicas, warnings, uptime)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                ts,
                s[CounterKey.MESSAGES.value],
                s[CounterKey.ERRORS.value],
                s[CounterKey.RETRIES.value],
                s[CounterKey.HANDLES.value],
                s[CounterKey.RETRACTED.value],
                s[CounterKey.REPLICAS.value],
                s[CounterKey.WARNINGS.value],
                s["uptime"],
            ),
        )
        self._conn.commit()

    # --- Enum-based internal access ---
    def __getitem__(self, key: CounterKey):
        return self._counters[key]

    # --- Properties for convenience ---
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


# Singleton instance
stats = Stats()
