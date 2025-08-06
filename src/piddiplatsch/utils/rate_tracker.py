import time
import logging
from tqdm import tqdm

logger = logging.getLogger(__name__)

class BaseRateTracker:
    def tick(self, n=1):
        raise NotImplementedError

    def close(self):
        pass


class LoggingRateTracker(BaseRateTracker):
    def __init__(self, name="rate", log_interval=10):
        self.name = name
        self.log_interval = log_interval
        self.start_time = time.time()
        self.last_log_time = self.start_time
        self.count = 0

    def tick(self, n=1):
        self.count += n
        now = time.time()
        elapsed = now - self.last_log_time
        if elapsed >= self.log_interval:
            total_elapsed = now - self.start_time
            rate = self.count / total_elapsed if total_elapsed > 0 else 0
            logger.info(f"[{self.name}] Total: {self.count}, Rate: {rate:.2f}/s")
            self.last_log_time = now


class TqdmRateTracker(BaseRateTracker):
    def __init__(self, name="progress", update_interval=5):
        self.name = name
        self.start_time = time.time()
        self.last_update = self.start_time
        self.count = 0
        self.update_interval = update_interval  # seconds

        self.bar = tqdm(
            total=0,  # No total → acts like a ticker
            desc=self._format_desc(0, 0),
            bar_format="{desc}",  # Don't show a progress bar, just the text
            dynamic_ncols=True,
        )

    def _format_desc(self, elapsed, rate):
        h, rem = divmod(int(elapsed), 3600)
        m, s = divmod(rem, 60)
        return f"{self.name:<10} | {self.count:>6} msgs | {rate:6.1f}/min | {h:02}:{m:02}:{s:02} elapsed"

    def tick(self, n=1):
        self.count += n

        now = time.time()
        elapsed = now - self.start_time

        if now - self.last_update >= self.update_interval:
            rate_per_min = (self.count / elapsed) * 60 if elapsed > 0 else 0
            self.bar.set_description(self._format_desc(elapsed, rate_per_min))
            self.last_update = now

    def close(self):
        elapsed = time.time() - self.start_time
        final_rate = (self.count / elapsed) * 60 if elapsed > 0 else 0
        self.bar.set_description(self._format_desc(elapsed, final_rate))
        self.bar.close()


def get_rate_tracker(name="rate", use_tqdm=False, log_interval=10):
    if use_tqdm:
        return TqdmRateTracker(name)
    else:
        return LoggingRateTracker(name, log_interval=log_interval)
