import time
import logging

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
    def __init__(self, name="rate"):
        from tqdm import tqdm
        self.bar = tqdm(desc=name, unit="msg", dynamic_ncols=True)

    def tick(self, n=1):
        self.bar.update(n)

    def close(self):
        self.bar.close()


def get_rate_tracker(name="rate", use_tqdm=False, log_interval=10):
    if use_tqdm:
        return TqdmRateTracker(name)
    else:
        return LoggingRateTracker(name, log_interval=log_interval)
