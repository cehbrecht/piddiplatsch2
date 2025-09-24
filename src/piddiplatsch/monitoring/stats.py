import datetime
import logging
import sqlite3
import time
from enum import Enum

from piddiplatsch.config import config

logger = logging.getLogger(__name__)


def to_iso(dt: float | datetime.datetime | None) -> str | None:
    """Convert a timestamp or datetime to a UTC ISO 8601 string."""
    if dt is None:
        return None
    if isinstance(dt, datetime.datetime):
        # If naive, assume UTC; otherwise convert to UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        else:
            dt = dt.astimezone(datetime.timezone.utc)
        return dt.isoformat()
    # dt is a float timestamp
    return datetime.datetime.fromtimestamp(dt, tz=datetime.timezone.utc).isoformat()


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
    SKIPPED = "skipped_messages"
    HANDLE_TIME = "total_handle_processing_time"  # float seconds


# -----------------------
# Reporter base
# -----------------------
class StatsReporter:
    def log(self, summary: dict):
        raise NotImplementedError

    def close(self):
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
                skipped_messages INTEGER,
                total_handle_processing_time REAL,
                uptime REAL,
                message_rate REAL,
                handle_rate REAL,
                messages_per_sec REAL
            )
            """
        )
        self._cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_message_stats_ts ON message_stats(ts)"
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
                                       retracted_messages, replicas, warnings,
                                       skipped_messages, total_handle_processing_time,
                                       uptime, message_rate, handle_rate, messages_per_sec)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                summary[CounterKey.SKIPPED.value],
                summary[CounterKey.HANDLE_TIME.value],
                summary["uptime"],
                summary["message_rate"],
                summary["handle_rate"],
                summary["messages_per_sec"],
            ),
        )
        self._conn.commit()

    def close(self):
        if getattr(self, "_closed", False):
            return
        try:
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
        log_interval_seconds: int | None = None,
        log_interval_messages: int | None = None,
        db_path: str | None = None,
        enable_db: bool = False,
    ):
        if getattr(self, "_initialized", False):
            return

        # Private timestamps as UTC datetime
        self._start_time = datetime.datetime.now(datetime.UTC)
        self._last_message_time: datetime.datetime | None = None
        self._last_error_time: datetime.datetime | None = None

        # Initialize counters
        self._counters = dict.fromkeys(CounterKey, 0)
        self._counters[CounterKey.HANDLE_TIME] = 0.0

        # Logging control
        self.log_interval_seconds = log_interval_seconds or 10
        self.log_interval_messages = log_interval_messages or 100
        self._last_log_time = time.time()
        self._last_logged_messages = 0

        # Reporters
        self.reporters: list[StatsReporter] = [ConsoleReporter()]
        if enable_db and db_path:
            self.reporters.append(SQLiteReporter(db_path=db_path))

        self._closed = False
        self._initialized = True

    # --- Core increment ---
    def increment(self, key: CounterKey, n=1):
        if key == CounterKey.HANDLE_TIME:
            self._counters[key] += n
        else:
            self._counters[key] += n

        if key == CounterKey.MESSAGES:
            self._last_message_time = datetime.datetime.utcnow()
            self._maybe_log()

    # --- Shortcuts ---
    def tick(self, n=1):
        self.increment(CounterKey.MESSAGES, n)

    def retry(self, n=1):
        self.increment(CounterKey.RETRIES, n)

    def handle(self, n=1, handle_time_sec: float = 0.0):
        self.increment(CounterKey.HANDLES, n)
        if handle_time_sec > 0:
            self.increment(CounterKey.HANDLE_TIME, handle_time_sec)

    def error(self, message: str | None = None, n=1):
        self.increment(CounterKey.ERRORS, n)
        self._last_error_time = datetime.datetime.utcnow()
        if message:
            logger.error(f"ERROR: {message}")

    def retracted(self, message: str | None = None, n=1):
        self.increment(CounterKey.RETRACTED, n)
        if message:
            logger.warning(f"RETRACTED: {message}")

    def replica(self, message: str | None = None, n=1):
        self.increment(CounterKey.REPLICAS, n)
        if message:
            logger.info(f"REPLICA: {message}")

    def warn(self, message: str | None = None, n=1):
        self.increment(CounterKey.WARNINGS, n)
        if message:
            logger.warning(f"WARNING: {message}")

    def skip(self, n=1):
        self.increment(CounterKey.SKIPPED, n)

    # --- Logging / persistence ---
    def _maybe_log(self):
        now = time.time()
        messages_since_last = (
            self._counters[CounterKey.MESSAGES] - self._last_logged_messages
        )

        if messages_since_last == 0:
            return

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
    def messages(self) -> int:
        return self._counters[CounterKey.MESSAGES]

    @property
    def replicas(self) -> int:
        return self._counters[CounterKey.REPLICAS]

    @property
    def retracted_messages(self) -> int:
        return self._counters[CounterKey.RETRACTED]

    @property
    def skipped_messages(self) -> int:
        return self._counters[CounterKey.SKIPPED]

    @property
    def errors(self) -> int:
        return self._counters[CounterKey.ERRORS]

    @property
    def warnings(self) -> int:
        return self._counters[CounterKey.WARNINGS]

    @property
    def retries(self) -> int:
        return self._counters[CounterKey.RETRIES]

    @property
    def handles(self) -> int:
        return self._counters[CounterKey.HANDLES]

    @property
    def handle_time_total(self) -> float:
        return self._counters[CounterKey.HANDLE_TIME]

    @property
    def start_time(self) -> datetime.datetime:
        return self._start_time

    @property
    def uptime(self) -> float:
        return (datetime.datetime.utcnow() - self._start_time).total_seconds()

    @property
    def last_message_time(self) -> datetime.datetime | None:
        return self._last_message_time

    @property
    def last_error_time(self) -> datetime.datetime | None:
        return self._last_error_time

    @property
    def message_rate(self) -> float:
        return self.messages / self.uptime if self.uptime > 0 else 0.0

    @property
    def handle_rate(self) -> float:
        return self.handles / self.uptime if self.uptime > 0 else 0.0

    @property
    def messages_per_sec(self) -> float:
        interval = time.time() - self._last_log_time
        interval_messages = self.messages - self._last_logged_messages
        return interval_messages / interval if interval > 0 else 0.0

    # --- Summary ---
    def summary(self):
        summary = {key.value: self._counters[key] for key in CounterKey}
        summary.update(
            {
                "uptime": self.uptime,
                "message_rate": self.message_rate,
                "handle_rate": self.handle_rate,
                "messages_per_sec": self.messages_per_sec,
                "last_message_time": to_iso(self.last_message_time),
                "last_error_time": to_iso(self.last_error_time),
                "start_time": to_iso(self.start_time),
            }
        )
        return summary

    # --- Cleanup ---
    def close(self):
        if getattr(self, "_closed", False):
            return
        for reporter in list(self.reporters):
            try:
                reporter.close()
            except Exception:
                logger.exception("Error closing reporter %s", reporter)
        self._closed = True


# -----------------------
# Singleton instance
# -----------------------
stats_config = config.get("stats", {})

stats = Stats(
    log_interval_seconds=stats_config.get("interval_seconds"),
    log_interval_messages=stats_config.get("summary_interval"),
    db_path=stats_config.get("db_path"),
    enable_db=stats_config.get("enable_db", False),
)
