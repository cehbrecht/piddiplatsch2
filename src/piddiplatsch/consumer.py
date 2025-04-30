import logging
import json
import uuid
from typing import Generator, Tuple

from confluent_kafka import Consumer as ConfluentConsumer, KafkaException
from piddiplatsch.handle_client import HandleClient
from piddiplatsch.config import config

# Logging is configured globally once
config.configure_logging()


class Consumer:
    def __init__(
        self, topic: str, kafka_server: str, group_id: str = "piddiplatsch-consumer"
    ):
        """Initialize the Kafka Consumer."""
        self.topic = topic
        self.kafka_server = kafka_server
        self.group_id = group_id

        self.consumer = ConfluentConsumer(
            {
                "bootstrap.servers": kafka_server,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": True,
            }
        )
        self.consumer.subscribe([topic])

        self.handle_client = HandleClient(
            server_url=config.get("handle", "server_url"),
            prefix=config.get("handle", "prefix"),
            username=config.get("handle", "username"),
            password=config.get("handle", "password"),
        )

    def consume(self) -> Generator[Tuple[str, dict], None, None]:
        """Generator that yields messages from Kafka."""
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())

                key = msg.key().decode("utf-8")
                logging.debug(f"Got a message: {key}")

                try:
                    value = json.loads(msg.value().decode("utf-8"))
                    yield key, value
                except json.JSONDecodeError as e:
                    logging.error(f"Invalid JSON in message with key {key}: {e}")
        finally:
            self.consumer.close()

    def process_message(self, key: str, value: dict):
        """High-level message processing entry point."""
        logging.info(f"Processing message: {key}")

        pid = self._build_pid(key, value)
        record = self._build_record(value)
        self._add_item(pid, record)

    def _build_pid(self, key: str, value: dict) -> str:
        """Extract or generate a PID from the message."""
        try:
            return value["data"]["payload"]["item"]["id"]
        except (KeyError, TypeError):
            return str(uuid.uuid5(uuid.NAMESPACE_DNS, key))

    def _build_record(self, value: dict) -> dict:
        """Build the record to be registered with the Handle Service."""
        try:
            url = value["data"]["payload"]["item"]["links"][0]["href"]
        except (KeyError, IndexError, TypeError):
            url = None

        return {
            "URL": url,
            "CHECKSUM": None,
        }

    def _add_item(self, pid: str, record: dict):
        """Add an item to the Handle Service."""
        logging.info(f"Adding item to Handle Service: pid = {pid}, record = {record}")
        try:
            self.handle_client.add_item(pid, record)
            logging.info(f"✅ Added item: pid = {pid}")
        except Exception as e:
            logging.error(f"❌ Failed to add item with pid = {pid}: {e}")


# External entry point
def process_message(key, value):
    """Shim for legacy CLI usage."""
    consumer = Consumer(topic="unused", kafka_server="unused")
    consumer.process_message(key, value)
