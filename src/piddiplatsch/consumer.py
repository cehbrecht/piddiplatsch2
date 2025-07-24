import logging
import json
import signal
import sys
from datetime import datetime
from confluent_kafka import Consumer as ConfluentConsumer, KafkaException
from piddiplatsch.handle_client import HandleClient
from piddiplatsch.recovery import FailureRecovery
from piddiplatsch.dump import DumpRecorder
from piddiplatsch.stats import StatsTracker

from piddiplatsch.plugin_loader import load_single_plugin

logger = logging.getLogger(__name__)


class Consumer:
    def __init__(self, topic: str, kafka_cfg: dict):
        self.topic = topic
        self.consumer = ConfluentConsumer(kafka_cfg)
        self.consumer.subscribe([self.topic])

    def consume(self):
        """Yield messages from Kafka."""
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
    """Encapsulates the Kafka consumer, processor, and handle client."""

    def __init__(
        self, topic: str, kafka_cfg: dict, processor: str, dump_messages: bool = False
    ):
        self.consumer = Consumer(topic, kafka_cfg)
        self.handle_client = HandleClient.from_config()
        self.processor = load_single_plugin(processor)
        self.dump_messages = dump_messages
        self.stats = StatsTracker(summary_interval=100)

    def run(self):
        """Consume and process messages indefinitely."""
        logger.info("Starting consumer pipeline...")
        for key, value in self.consumer.consume():
            self.process_message(key, value)

    def process_message(self, key: str, value: dict):
        """Process a single message."""
        try:
            logger.info(f"Processing message: {key}")
            start = datetime.now()
            if self.dump_messages:
                DumpRecorder.record_item(key, value)
            self.processor.process(key, value, self.handle_client)
            logger.debug(f"Processing message ... done: {key}")

            elapsed = (datetime.now() - start).total_seconds()
            self.stats.record_success(key, 1, elapsed)
        except Exception as e:
            logger.error(f"Error processing message {key}: {e}")
            retries = value.get("retries", 0)
            FailureRecovery.record_failed_item(key, value, retries=retries)
            self.stats.record_failure(key, e)

    def stop(self):
        """Gracefully stop the consumer."""
        logger.warning("Stopping consumer...")
        # Any other cleanup logic can be added here if needed.
        self.stats.log_summary()


def start_consumer(
    topic: str, kafka_cfg: dict, processor: str, dump_messages: bool = False
):
    pipeline = ConsumerPipeline(
        topic, kafka_cfg, processor, dump_messages=dump_messages
    )

    # Handle graceful shutdown
    def sigint_handler(signal, frame):
        logger.warning("Received SIGINT. Gracefully shutting down.")
        pipeline.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, sigint_handler)  # Handle Ctrl+C (SIGINT)

    try:
        pipeline.run()
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted.")
        pipeline.stop()
        sys.exit(0)
