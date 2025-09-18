import time
from datetime import datetime, timezone

import humanize
from tqdm import tqdm

from piddiplatsch.monitoring.stats import stats


class BaseProgress:
    """Base class for progress display."""

    def refresh(self):
        raise NotImplementedError

    def close(self):
        pass


class NoOpProgress(BaseProgress):
    """Dummy progress display; does nothing."""

    def refresh(self):
        pass


class Progress(BaseProgress):
    """Displays message stats in the console (tqdm-based) with timestamps and total runtime."""

    def __init__(self, title="progress", update_interval=5):
        self.title = title
        self.update_interval = update_interval
        self.last_update = time.time()

        self.bar = tqdm(
            total=0,  # ticker mode, no total
            desc=self._format_desc(),
            bar_format="{desc}",
            dynamic_ncols=True,
        )

    def _to_utc_dt(self, ts):
        if ts is None:
            return None
        if isinstance(ts, float):
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        if ts.tzinfo is None:
            return ts.replace(tzinfo=timezone.utc)
        return ts

    def _format_time(self, ts):
        dt = self._to_utc_dt(ts)
        return dt.strftime("%H:%M:%S") if dt else "--:--:--"

    def _time_ago(self, ts):
        dt = self._to_utc_dt(ts)
        if dt is None:
            return "--"
        return humanize.naturaltime(datetime.now(timezone.utc) - dt)

    def _format_elapsed(self, start_ts):
        start_dt = self._to_utc_dt(start_ts)
        if start_dt is None:
            return "--:--:--"
        elapsed = int((datetime.now(timezone.utc) - start_dt).total_seconds())
        h, rem = divmod(elapsed, 3600)
        m, s = divmod(rem, 60)
        return f"{h:02}:{m:02}:{s:02}"

    def _format_desc(self):
        return (
            f"{self.title:<10} | msgs: {stats.messages} ({stats.message_rate:.2f}/s) "
            f"| hndls: {stats.handles} ({stats.handle_rate:.2f}/s) "
            f"| err: {stats.errors} | warn: {stats.warnings} "
            f"| rtrct: {stats.retracted_messages} | rplc: {stats.replicas} "
            f"| last_err: {self._time_ago(stats.last_error_time)} "
            f"| elapsed: {self._format_elapsed(stats.start_time)}"
        )

    def refresh(self):
        """Update the display from Stats."""
        now = time.time()
        if now - self.last_update >= self.update_interval:
            self.bar.set_description(self._format_desc())
            self.last_update = now

    def close(self):
        self.bar.set_description(self._format_desc())
        self.bar.close()


def get_progress(title="progress", use_tqdm=False, update_interval=5):
    """
    Factory to get a progress display.
    - Returns Progress (tqdm-based) if use_tqdm=True
    - Returns NoOpProgress if use_tqdm=False
    """
    if use_tqdm:
        return Progress(title=title, update_interval=update_interval)
    else:
        return NoOpProgress()
