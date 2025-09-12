import time
from datetime import datetime

import humanize
from tqdm import tqdm

from piddiplatsch.monitoring.stats import MessageStats


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

    def __init__(self, title="progress", stats: MessageStats = None, update_interval=5):
        self.title = title
        self._stats = stats or MessageStats()
        self.update_interval = update_interval
        self.start_time = self._stats.start_time
        self.last_update = time.time()

        self.bar = tqdm(
            total=0,  # ticker mode, no total
            desc=self._format_desc(),
            bar_format="{desc}",
            dynamic_ncols=True,
        )

    @property
    def stats(self):
        """Read-only access to MessageStats."""
        return self._stats

    def _format_time(self, ts):
        return time.strftime("%H:%M:%S", time.localtime(ts)) if ts else "--:--:--"

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
            f"{self.title:<10} | {self.stats.messages:>6} msgs | "
            f"{self.stats.errors:>4} errors | "
            f"start: {self._format_time(self.start_time)} | "
            f"last_msg: {self._format_time(self.stats.last_message_time)} "
            f"({self._time_ago(self.stats.last_message_time)}) | "
            f"last_err: {self._format_time(self.stats.last_error_time)} "
            f"({self._time_ago(self.stats.last_error_time)}) | "
            f"running: {self._format_elapsed(self.start_time)}"
        )

    def refresh(self):
        """Update the display from MessageStats."""
        now = time.time()
        if now - self.last_update >= self.update_interval:
            self.bar.set_description(self._format_desc())
            self.last_update = now

    def close(self):
        self.bar.set_description(self._format_desc())
        self.bar.close()


def get_progress(
    title="progress", use_tqdm=False, stats: MessageStats = None, update_interval=5
):
    """
    Factory to get a progress display.
    - Returns Progress (tqdm-based) if use_tqdm=True
    - Returns NoOpProgress if use_tqdm=False
    """
    if use_tqdm:
        return Progress(title=title, stats=stats, update_interval=update_interval)
    else:
        return NoOpProgress()
