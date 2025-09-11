import logging
import time

from tqdm import tqdm

from piddiplatsch.monitoring.base import MessageStats

logger = logging.getLogger(__name__)


class BaseRateTracker:
    def tick(self, n=1):
        raise NotImplementedError

    def close(self):
        pass


class DummyRateTracker(BaseRateTracker):
    def tick(self, n=1):
        pass

    def refresh(self):
        pass  # no-op


class TqdmRateTracker(BaseRateTracker):
    def __init__(self, name="progress", stats=None, update_interval=5):
        self.name = name
        self.stats = stats
        self.update_interval = update_interval
        self.start_time = time.time()
        self.last_update = self.start_time

        self.bar = tqdm(
            total=0,
            desc=self._format_desc(0, 0),
            bar_format="{desc}",
            dynamic_ncols=True,
        )

    def _format_desc(self, elapsed, rate):
        h, rem = divmod(int(elapsed), 3600)
        m, s = divmod(rem, 60)
        errors = self.stats.errors if self.stats else 0
        messages = self.stats.messages if self.stats else 0
        return (
            f"{self.name:<10} | {messages:>6} msgs | {rate:6.1f}/min "
            f"| {errors:>4} errors | {h:02}:{m:02}:{s:02} elapsed"
        )

    def refresh(self):
        """Update the tqdm display from stats."""
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


def get_rate_tracker(name="rate", use_tqdm=False, stats=None, update_interval=5):
    """
    Factory to create a rate tracker.
    If use_tqdm=True, returns TqdmRateTracker that reads from stats.
    If use_tqdm=False, returns DummyRateTracker.
    """
    if use_tqdm:
        if stats is None:
            stats = MessageStats()  # fallback if none provided
        return TqdmRateTracker(name=name, stats=stats, update_interval=update_interval)
    else:
        return DummyRateTracker()
