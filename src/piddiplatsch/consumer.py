import json
import logging
import signal
import sys

from confluent_kafka import Consumer as ConfluentConsumer
from confluent_kafka import KafkaException

from piddiplatsch.config import config
from piddiplatsch.dump import DumpRecorder
from piddiplatsch.monitoring import MetricsTracker, get_rate_tracker
from piddiplatsch.plugin_loader import load_single_plugin
from piddiplatsch.processing import ProcessingResult  # dataclass
from piddiplatsch.recovery import FailureRecovery

logger = logging.getLogger(__name__)


class MaxErrorsExceededError(Exception):
    """Raised when the consumer reaches its max error limit."""


class Consumer:
    """Thin wrapper around Kafka consumer."""

    def __init__(self, topic: str, kafka_cfg: dict):
        self.topic = topic
        self.consumer = ConfluentConsumer(kafka_cfg)
        self.consumer.subscribe([self.topic])

    def consume(self):
        """Yield decoded Kafka messages."""
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())

                key = msg.key().decode("utf-8") if msg.key() else None
                try:
                    value = json.loads(msg.value().decode("utf-8"))
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode message: {e}")
                    continue

                yield key, value
        finally:
            self.consumer.close()


class ConsumerPipeline:
    """Coordinates Kafka consumption, message processing, and metrics."""

    def __init__(
        self,
        topic: str,
        kafka_cfg: dict,
        processor: str,
        *,
        dump_messages=False,
        verbose=False,
        max_errors=-1,
    ):
        self.consumer = Consumer(topic, kafka_cfg)
        self.processor = load_single_plugin(processor)
        self.dump_messages = dump_messages
        self.metrics = MetricsTracker()
        self.message_tracker = get_rate_tracker("messages", use_tqdm=verbose)
        self.max_errors = int(max_errors)
        self._error_count = 0

    def run(self):
        """Consume and process messages until stopped or error limit reached."""
        logger.info("Starting consumer pipeline...")
        try:
            for key, value in self.consumer.consume():
                result = self._safe_process_message(key, value)
                self.metrics.record_result(result)
                self.message_tracker.tick()
        except MaxErrorsExceededError as e:
            logger.error(str(e))
            self.stop(reason="max_errors_exceeded")

    def _safe_process_message(self, key: str, value: dict) -> ProcessingResult:
        """Process a single message with error handling."""
        try:
            logger.debug(f"Processing message: {key}")

            if self.dump_messages:
                DumpRecorder.record_item(key, value)

            return self.processor.process(key, value)

        except Exception as e:
            logger.exception(f"Error processing message {key}")
            retries = value.get("retries", 0)
            reason = str(e)
            FailureRecovery.record_failed_item(
                key, value, retries=retries, reason=reason
            )

            self._error_count += 1
            if self.max_errors >= 0 and self._error_count >= self.max_errors:
                raise MaxErrorsExceededError(
                    f"Max error limit reached ({self._error_count}/{self.max_errors})"
                )

            return ProcessingResult(key=key, success=False, error=reason)

    def stop(self, reason: str = "manual"):
        """Gracefully stop the pipeline."""
        logger.warning(f"Stopping consumer (reason: {reason})...")
        self.metrics.log_summary()
        self.message_tracker.close()


def start_consumer(
    topic: str, kafka_cfg: dict, processor: str, *, dump_messages=False, verbose=False
):
    """Entry point for running the consumer."""
    max_errors = config.get("consumer", {}).get("max_errors", -1)

    pipeline = ConsumerPipeline(
        topic,
        kafka_cfg,
        processor,
        dump_messages=dump_messages,
        verbose=verbose,
        max_errors=max_errors,
    )

    def sigint_handler(sig, frame):
        logger.warning("Received SIGINT. Gracefully shutting down.")
        pipeline.stop(reason="sigint")
        sys.exit(0)

    signal.signal(signal.SIGINT, sigint_handler)

    try:
        pipeline.run()
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted.")
        pipeline.stop(reason="keyboard_interrupt")
        sys.exit(0)
