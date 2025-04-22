import logging
import json
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
        self, topic: str, kafka_server: str, group_id: str = "piddiplatsch-consumer-1"
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
                yield value
        finally:
            self.consumer.close()


def process_message(message_data):
    """Process a CMIP7 record message."""
    logging.info(f"Processing message: {message_data}")

    action = message_data.get("action")
    record = message_data.get("record")

    if action == "add":
        add_pid(record)
    elif action == "update":
        update_pid(record)
    elif action == "delete":
        delete_pid(record.get("pid"))


def add_pid(record):
    """Adds a PID to the Handle Service."""
    handle_client = build_client()
    pid = f"21.T11148/{record.get('pid')}"

    try:
        handle_client.add_pid(record)
        logging.info(f"Added PID {pid} for record: {record}")
    except Exception as e:
        logging.error(f"Failed to add PID {pid}: {e}")


def update_pid(record):
    """Updates a PID in the Handle Service."""
    handle_client = build_client()
    pid = record.get("pid")
    if pid:
        try:
            handle_client.update_pid(pid, record)
            logging.info(f"Updated PID {pid} for record: {record}")
        except Exception as e:
            logging.error(f"Failed to update PID {pid}: {e}")


def delete_pid(pid):
    """Deletes a PID from the Handle Service."""
    handle_client = build_client()
    try:
        handle_client.delete_pid(pid)
        logging.info(f"Deleted PID: {pid}")
    except Exception as e:
        logging.error(f"Failed to delete PID {pid}: {e}")
