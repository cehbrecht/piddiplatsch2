class MessageStats:
    """Tracks counts for messages, errors, retries, etc."""

    def __init__(self):
        self.messages = 0
        self.errors = 0
        self.retries = 0  # optional, can be updated by pipeline

    def tick(self, n=1):
        """Increment messages processed."""
        self.messages += n

    def error(self, n=1):
        """Increment errors encountered."""
        self.errors += n

    def retry(self, n=1):
        """Increment retries (optional)."""
        self.retries += n
