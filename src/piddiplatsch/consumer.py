import json
import logging
import signal
import sys
from enum import Enum

from confluent_kafka import Consumer as ConfluentConsumer
from confluent_kafka import KafkaException

from piddiplatsch.config import config
from piddiplatsch.dump import DumpRecorder
from piddiplatsch.exceptions import MaxErrorsExceededError
from piddiplatsch.monitoring import MetricsTracker, get_progress, stats
from piddiplatsch.plugin_loader import load_single_plugin
from piddiplatsch.processing import ProcessingResult
from piddiplatsch.recovery import FailureRecovery

logger = logging.getLogger(__name__)


class StopCause(str, Enum):
    MANUAL = "manual"
    SIGINT = "sigint"
    KEYBOARD_INTERRUPT = "keyboard_interrupt"
    MAX_ERRORS = "max_errors_exceeded"


class Consumer:
    """Thin wrapper around Kafka consumer."""

    def __init__(self, topic: str, kafka_cfg: dict):
        self.topic = topic
        self.consumer = ConfluentConsumer(kafka_cfg)
        self.consumer.subscribe([self.topic])

    def consume(self):
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
        topic,
        kafka_cfg,
        processor,
        *,
        dump_messages=False,
        verbose=False,
        max_errors=-1,
    ):
        self.consumer = Consumer(topic, kafka_cfg)
        self.processor = load_single_plugin(processor)
        self.dump_messages = dump_messages
        self.metrics = MetricsTracker()
        self.max_errors = int(max_errors)
        self.progress = get_progress("messages", use_tqdm=verbose)

    def run(self):
        logger.info("Starting consumer pipeline...")
        for key, value in self.consumer.consume():
            result = self._safe_process_message(key, value)
            self.metrics.record_result(result)

            # Count messages
            stats.tick()

            # Count errors
            if not result.success:
                stats.error()

            # Refresh progress display (does NOT increment messages)
            self.progress.refresh()

            # Check max errors
            self._check_success(result)

    def _check_success(self, result: ProcessingResult):
        if (
            not result.success
            and self.max_errors >= 0
            and stats.errors >= self.max_errors
        ):
            raise MaxErrorsExceededError(
                f"Max error limit reached ({stats.errors}/{self.max_errors})"
            )

    def _safe_process_message(self, key, value):
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
            return ProcessingResult(key=key, success=False, error=reason)

    def stop(self, cause: StopCause = StopCause.MANUAL):
        logger.warning(f"Stopping consumer (cause: {cause.value})...")
        self.metrics.log_summary()
        self.progress.close()
        logger.info(f"Total messages: {stats.messages}, total errors: {stats.errors}")


def start_consumer(topic, kafka_cfg, processor, *, dump_messages=False, verbose=False):
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
        pipeline.stop(cause=StopCause.SIGINT)
        sys.exit(0)

    signal.signal(signal.SIGINT, sigint_handler)

    try:
        pipeline.run()
    except MaxErrorsExceededError as e:
        logger.error(str(e))
        pipeline.stop(cause=StopCause.MAX_ERRORS)
        sys.exit(1)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted.")
        pipeline.stop(cause=StopCause.KEYBOARD_INTERRUPT)
        sys.exit(0)
