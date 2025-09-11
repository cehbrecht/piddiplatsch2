import logging
import time

from tqdm import tqdm

from piddiplatsch.monitoring.base import MessageStats

logger = logging.getLogger(__name__)


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
    """Displays message stats in the console (tqdm-based)."""

    def __init__(self, name="progress", stats: MessageStats = None, update_interval=5):
        self.name = name
        self.stats = stats or MessageStats()
        self.update_interval = update_interval
        self.start_time = time.time()
        self.last_update = self.start_time

        self.bar = tqdm(
            total=0,  # ticker mode, no total
            desc=self._format_desc(0, 0),
            bar_format="{desc}",
            dynamic_ncols=True,
        )

    def _format_desc(self, elapsed, rate):
        h, rem = divmod(int(elapsed), 3600)
        m, s = divmod(rem, 60)
        return (
            f"{self.name:<10} | {self.stats.messages:>6} msgs | {rate:6.1f}/min "
            f"| {self.stats.errors:>4} errors | {h:02}:{m:02}:{s:02} elapsed"
        )

    def refresh(self):
        """Update the display from MessageStats."""
        now = time.time()
        elapsed = now - self.start_time
        if now - self.last_update >= self.update_interval:
            rate_per_min = (self.stats.messages / elapsed) * 60 if elapsed > 0 else 0
            self.bar.set_description(self._format_desc(elapsed, rate_per_min))
            self.last_update = now

    def close(self):
        elapsed = time.time() - self.start_time
        rate_per_min = (self.stats.messages / elapsed) * 60 if elapsed > 0 else 0
        self.bar.set_description(self._format_desc(elapsed, rate_per_min))
        self.bar.close()


def get_progress(
    name="progress", use_tqdm=False, stats: MessageStats = None, update_interval=5
):
    """
    Factory to get a progress display.
    - Returns Progress (tqdm-based) if use_tqdm=True
    - Returns NoOpProgress if use_tqdm=False
    """
    if use_tqdm:
        return Progress(name=name, stats=stats, update_interval=update_interval)
    else:
        return NoOpProgress()
