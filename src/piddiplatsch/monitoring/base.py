import time


class MessageStats:
    """Tracks counts and timestamps for messages, errors, retries, etc."""

    def __init__(self):
        self.messages = 0
        self.errors = 0
        self.retries = 0  # optional
        self.start_time = time.time()
        self.last_message_time = None
        self.last_error_time = None

    def tick(self, n=1):
        """Increment messages processed."""
        self.messages += n
        self.last_message_time = time.time()

    def error(self, n=1):
        """Increment errors encountered."""
        self.errors += n
        self.last_error_time = time.time()

    def retry(self, n=1):
        """Increment retries (optional)."""
        self.retries += n
