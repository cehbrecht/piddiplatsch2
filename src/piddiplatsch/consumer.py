import logging
import json
from kafka import KafkaConsumer
from piddiplatsch.handle_client import HandleClient

class Consumer:
    def __init__(self, topic: str, kafka_server: str):
        """Initialize the Kafka Consumer."""
        self.topic = topic
        self.kafka_server = kafka_server

    def consume(self):
        """Consume messages from Kafka."""
        consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.kafka_server,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )
        for message in consumer:
            yield message


def process_message(message):
    """Process a CMIP7 record message."""
    logging.info(f"Processing message: {message}")
    action = message.get("action")
    record = message.get("record")

    if action == "add":
        add_pid(record)
    elif action == "update":
        update_pid(record)
    elif action == "delete":
        delete_pid(record.get("pid"))


def add_pid(record):
    """Adds a PID to the Handle Service."""
    handle_client = HandleClient()
    pid = f"21.T11148/{record.get('pid')}"
    location = record.get("location", None)

    try:
        handle_client.add_handle(pid, record, overwrite=True, location=location)
        logging.info(f"Added PID {pid} for record: {record}")
    except Exception as e:
        logging.error(f"Failed to add PID {pid}: {e}")


def update_pid(record):
    """Updates a PID in the Handle Service."""
    handle_client = HandleClient()
    pid = record.get("pid")
    if pid:
        try:
            handle_client.update_handle(pid, record)
            logging.info(f"Updated PID {pid} for record: {record}")
        except Exception as e:
            logging.error(f"Failed to update PID {pid}: {e}")


def delete_pid(pid):
    """Deletes a PID from the Handle Service."""
    handle_client = HandleClient()
    try:
        handle_client.delete_handle(pid)
        logging.info(f"Deleted PID: {pid}")
    except Exception as e:
        logging.error(f"Failed to delete PID {pid}: {e}")
