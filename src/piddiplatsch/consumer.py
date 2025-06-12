import logging
import json
import signal
import sys
from confluent_kafka import Consumer as ConfluentConsumer, KafkaException
from piddiplatsch.handle_client import HandleClient
from piddiplatsch.config import config as piddi_config
from piddiplatsch.plugin_loader import load_single_plugin

# Set up logging
piddi_config.configure_logging()


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
                    logging.error(f"Failed to decode message: {e}")
                    continue

                yield key, value
        finally:
            self.consumer.close()


class ConsumerPipeline:
    """Encapsulates the Kafka consumer, processor, and handle client."""

    def __init__(self, topic: str, kafka_cfg: dict, processor: str):
        self.consumer = Consumer(topic, kafka_cfg)
        self.handle_client = HandleClient.from_config()
        self.processor = load_single_plugin(processor)

    def run(self):
        """Consume and process messages indefinitely."""
        logging.info("Starting consumer pipeline...")
        for key, value in self.consumer.consume():
            self.process_message(key, value)

    def process_message(self, key: str, value: dict):
        """Process a single message."""
        try:
            logging.info(f"Processing message: {key}")
            self.processor.process(key, value, self.handle_client)
            logging.info(f"Processing message ... done: {key}")
        except Exception as e:
            logging.error(f"Error processing message {key}: {e}")
            # raise

    def stop(self):
        """Gracefully stop the consumer."""
        logging.info("Stopping consumer...")
        # Any other cleanup logic can be added here if needed.


def start_consumer(topic: str, kafka_cfg: dict, processor: str):
    pipeline = ConsumerPipeline(topic, kafka_cfg, processor)

    # Handle graceful shutdown
    def sigint_handler(signal, frame):
        logging.info("Received SIGINT. Gracefully shutting down.")
        pipeline.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, sigint_handler)  # Handle Ctrl+C (SIGINT)

    try:
        pipeline.run()
    except KeyboardInterrupt:
        logging.info("Consumer interrupted.")
        pipeline.stop()
        sys.exit(0)
