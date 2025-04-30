import logging
import json
from confluent_kafka import Consumer as ConfluentConsumer, KafkaException
from piddiplatsch.handle_client import HandleClient
from piddiplatsch.config import config
from piddiplatsch.plugin import load_processor

# Set up logging
config.configure_logging()


def build_client():
    """Create and return a HandleClient instance using configured credentials."""
    return HandleClient(
        server_url=config.get("handle", "server_url"),
        prefix=config.get("handle", "prefix"),
        username=config.get("handle", "username"),
        password=config.get("handle", "password"),
    )


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


def start_consumer(topic: str, kafka_server: str):
    """Start the Kafka consumer loop using a plugin-based processor."""
    handle_client = build_client()
    processor = load_processor()

    consumer = Consumer(topic, kafka_server)
    for key, value in consumer.consume():
        try:
            if processor.can_process(key, value):
                logging.info(f"Processing message: {key}")
                processor.process(key, value, handle_client)
            else:
                logging.debug(f"Ignoring message: {key}")
        except Exception as e:
            logging.error(f"Error processing message {key}: {e}")
            raise
