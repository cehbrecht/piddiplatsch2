import logging
import json
import uuid
from confluent_kafka import Consumer as ConfluentConsumer, KafkaException
from piddiplatsch.handle_client import HandleClient

# Handle Service Configuration (same as in handle_client.py)
HANDLE_SERVER_URL = "http://localhost:5000"  # Mock server for testing
HANDLE_PREFIX = "21.T11148"
USERNAME = "testuser"
PASSWORD = "testpass"


def build_client():
    """Create and return a HandleClient instance."""
    return HandleClient(
        server_url=HANDLE_SERVER_URL,
        prefix=HANDLE_PREFIX,
        username=USERNAME,
        password=PASSWORD,
    )


class Consumer:
    def __init__(
        self, topic: str, kafka_server: str, group_id: str = "piddiplatsch-consumer-4"
    ):
        """Initialize the Kafka Consumer."""
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
        """Consume messages from Kafka."""
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                print(f"consumed message: {msg}")
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())

                # Get the message key
                key = msg.key().decode("utf-8")
                print(f"got a message: {key}")

                # Parse the JSON payload
                value = json.loads(msg.value().decode("utf-8"))
                yield key, value
        finally:
            self.consumer.close()


def process_message(key, value):
    """Process a CMIP7 record message."""
    logging.info(f"Processing message: {key}")

    pid = build_pid(key, value)
    record = build_record(value)
    add_item(pid, record)


def build_pid(key, value):
    try:
        id = value["data"]["payload"]["item"]["id"]
    except Exception:
        id = str(uuid.uuid5(uuid.NAMESPACE_DNS, key))
    pid = f"{HANDLE_PREFIX}/{id}"
    return pid


def build_record(value):
    location = value["data"]["payload"]["item"]["links"][0]["href"]
    record = {"location": location}
    return record


def add_item(pid, record):
    """Adds an item with pid and record to the Handle Service."""
    logging.info(f"add pid = {pid}, record = {record}")
    handle_client = build_client()

    try:
        handle_client.add_item(pid, record)
        logging.info(f"Added PID {pid}")
    except Exception as e:
        logging.error(f"Failed to add PID {pid}: {e}")
