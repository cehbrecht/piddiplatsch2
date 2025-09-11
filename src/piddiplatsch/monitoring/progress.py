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

    def __init__(
        self,
        title="progress",
        stats: MessageStats = None,
        update_interval=5,
        show_retries=False,
    ):
        self.title = title
        self._stats = stats or MessageStats()
        self.update_interval = update_interval
        self.show_retries = show_retries
        self.start_time = time.time()
        self.last_update = self.start_time

        self.bar = tqdm(
            total=0,  # ticker mode, no total
            desc=self._format_desc(0, 0),
            bar_format="{desc}",
            dynamic_ncols=True,
        )

    @property
    def stats(self):
        """Read-only access to MessageStats."""
        return self._stats

    def _format_desc(self, elapsed, rate):
        h, rem = divmod(int(elapsed), 3600)
        m, s = divmod(rem, 60)
        desc = (
            f"{self.title:<10} | {self.stats.messages:>6} msgs | "
            f"{rate:6.1f}/min | {self.stats.errors:>4} errors | "
            f"{h:02}:{m:02}:{s:02} elapsed"
        )
        if self.show_retries:
            desc += f" | {self.stats.retries:>4} retries"
        return desc

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
    title="progress",
    use_tqdm=False,
    stats: MessageStats = None,
    update_interval=5,
    show_retries=False,
):
    """
    Factory to get a progress display.
    - Returns Progress (tqdm-based) if use_tqdm=True
    - Returns NoOpProgress if use_tqdm=False
    """
    if use_tqdm:
        return Progress(
            title=title,
            stats=stats,
            update_interval=update_interval,
            show_retries=show_retries,
        )
    else:
        return NoOpProgress()
