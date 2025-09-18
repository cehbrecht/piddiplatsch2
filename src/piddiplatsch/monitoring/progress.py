import time
from datetime import datetime

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

    def _format_time(self, ts):
        if ts is None:
            return "--:--:--"
        return time.strftime("%H:%M:%S", time.localtime(ts))

    def _time_ago(self, ts):
        """Return a human-readable relative time, e.g., '2 minutes ago'."""
        if ts is None:
            return "--"
        dt = datetime.fromtimestamp(ts)
        return humanize.naturaltime(datetime.now() - dt)

    def _format_elapsed(self, start_ts):
        elapsed = int(time.time() - start_ts)
        h, rem = divmod(elapsed, 3600)
        m, s = divmod(rem, 60)
        return f"{h:02}:{m:02}:{s:02}"

    def _format_desc(self):
        return (
            f"{self.title:<10} | {stats.messages:>6} msgs "
            f"({stats.message_rate:.2f}/s) | "
            f"{stats.handles:>4} handles "
            f"({stats.handle_rate:.2f}/s) | "
            f"{stats.errors:>4} errors | "
            f"{stats.warnings:>4} warns | "
            f"{stats.retracted_messages:>4} retracted | "
            f"{stats.replicas:>4} replicas | "
            f"start: {self._format_time(stats.start_time)} | "
            f"last_msg: {self._format_time(stats.last_message_time)} "
            f"({self._time_ago(stats.last_message_time)}) | "
            f"last_err: {self._format_time(stats.last_error_time)} "
            f"({self._time_ago(stats.last_error_time)}) | "
            f"running: {self._format_elapsed(stats.start_time)}"
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
