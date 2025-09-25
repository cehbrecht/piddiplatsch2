import json
import logging
import signal
import sys
from enum import StrEnum
from pathlib import Path

from confluent_kafka import Consumer as ConfluentConsumer
from confluent_kafka import KafkaException

from piddiplatsch.config import config
from piddiplatsch.exceptions import MaxErrorsExceededError
from piddiplatsch.monitoring.stats import CounterKey, stats
from piddiplatsch.persist.dump import DumpRecorder
from piddiplatsch.persist.recovery import FailureRecovery
from piddiplatsch.plugin_loader import load_single_plugin
from piddiplatsch.processing import ProcessingResult

logger = logging.getLogger(__name__)


class StopCause(StrEnum):
    MANUAL = "manual"
    SIGINT = "sigint"
    KEYBOARD_INTERRUPT = "keyboard_interrupt"
    MAX_ERRORS = "max_errors_exceeded"


# ----------------------------
# Base Consumer
# ----------------------------


class BaseConsumer:
    """Abstract base consumer interface."""

    def consume(self):
        """
        Yield tuples of (key, value) messages.
        Must be implemented by subclasses.
        """
        raise NotImplementedError


# ----------------------------
# Kafka Consumer
# ----------------------------


class KafkaConsumer(BaseConsumer):
    """Kafka consumer wrapper."""

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


# ----------------------------
# Direct Consumer (for tests / recovery)
# ----------------------------


class DirectConsumer(BaseConsumer):
    """Feed messages directly without Kafka."""

    def __init__(self, messages):
        """
        messages: iterable of (key, value) tuples
        """
        self.messages = list(messages)

    def consume(self):
        yield from self.messages


# ----------------------------
# Consumer Pipeline
# ----------------------------


class ConsumerPipeline:
    """Coordinates consumption, message processing, and stats."""

    def __init__(
        self,
        consumer: BaseConsumer,
        processor,
        *,
        dump_messages=False,
        verbose=False,
        max_errors=-1,
    ):
        """
        consumer: instance of BaseConsumer (KafkaConsumer or DirectConsumer)
        processor: plugin name
        """
        self.consumer = consumer
        self.processor = load_single_plugin(processor)
        self.dump_messages = dump_messages
        self.max_errors = int(max_errors)

        self.stats = stats
        self.progress = None
        try:
            from piddiplatsch.monitoring import get_progress

            self.progress = get_progress("messages", use_tqdm=verbose)
        except ImportError:
            pass

    def run(self):
        logger.info("Starting consumer pipeline...")
        for key, value in self.consumer.consume():
            result = self._safe_process_message(key, value)

            # Track metrics
            if result.success:
                self.stats.tick()
                self.stats.handle(
                    n=getattr(result, "num_handles", 0),
                    handle_time_sec=getattr(result, "handle_processing_time", 0.0),
                )
            else:
                self.stats.error(message=getattr(result, "error", None))

            if getattr(result, "skipped", False):
                self.stats.skip()

            if getattr(result, "retracted", False):
                self.stats.retracted(message=f"Dataset {key} retracted")

            if getattr(result, "replica", False):
                self.stats.replica(message=f"Dataset {key} replica created")

            if self.progress:
                self.progress.refresh()

            self._check_success()

    def _check_success(self):
        if self.max_errors >= 0 and self.stats.errors >= self.max_errors:
            raise MaxErrorsExceededError(
                f"Max error limit reached ({self.stats.errors}/{self.max_errors})"
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
        self.stats._log_stats()
        if self.progress:
            self.progress.close()
        logger.info(
            f"Total messages: {self.stats.messages}, total errors: {self.stats.errors}, "
            f"handles: {self.stats[CounterKey.HANDLES]}, skipped: {self.stats.skipped_messages}"
        )
        self.stats.close()


# ----------------------------
# Direct helpers for testing / recovery
# ----------------------------


def feed_messages_direct(messages, processor="cmip6"):
    consumer = DirectConsumer(messages)
    pipeline = ConsumerPipeline(consumer, processor=processor)
    pipeline.run()


def feed_test_files(testfile_paths, processor="cmip6"):
    messages = []
    for path in testfile_paths:
        if isinstance(path, str):
            path = Path(path)
        with path.open("r", encoding="utf-8") as f:
            messages.append((path.name, json.load(f)))
    feed_messages_direct(messages, processor=processor)


# ----------------------------
# CLI / Dispatcher entrypoint
# ----------------------------


def start_consumer(
    topic=None,
    kafka_cfg=None,
    processor="cmip6",
    *,
    dump_messages=False,
    verbose=False,
    enable_db=False,
    db_path: str | None = None,
    direct_messages=None,
):
    if enable_db:
        stats.__init__(db_path=db_path)
    else:
        stats.__init__()

    if direct_messages is not None:
        consumer = DirectConsumer(direct_messages)
    elif topic and kafka_cfg:
        consumer = KafkaConsumer(topic, kafka_cfg)
    else:
        raise ValueError("Either Kafka config or direct_messages must be provided")

    max_errors = config.get("consumer", {}).get("max_errors", -1)
    pipeline = ConsumerPipeline(
        consumer,
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
