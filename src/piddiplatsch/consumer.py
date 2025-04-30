import logging
import json
from confluent_kafka import Consumer as ConfluentConsumer, KafkaException
from piddiplatsch.handle_client import HandleClient
from piddiplatsch.config import config
from piddiplatsch.plugin import load_processor

# Set up logging
config.configure_logging()


class Consumer:
    def __init__(
        self, topic: str, kafka_server: str, group_id: str = "piddiplatsch-consumer"
    ):
        self.topic = topic
        self.kafka_server = kafka_server
        self.group_id = group_id
        self.consumer = ConfluentConsumer(
            {
                "bootstrap.servers": self.kafka_server,
                "group.id": self.group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": True,
            }
        )
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

    def __init__(self, topic: str, kafka_server: str):
        self.consumer = Consumer(topic, kafka_server)
        self.handle_client = HandleClient.from_config()
        self.processor = load_processor()

    def run(self):
        """Consume and process messages indefinitely."""
        for key, value in self.consumer.consume():
            self.process_message(key, value)

    def process_message(self, key: str, value: dict):
        """Process a single message."""
        try:
            logging.info(f"Processing message: {key}")
            self.processor.process(key, value, self.handle_client)
        except Exception as e:
            logging.error(f"Error processing message {key}: {e}")
            raise


def start_consumer(topic: str, kafka_server: str):
    pipeline = ConsumerPipeline(topic, kafka_server)
    pipeline.run()
