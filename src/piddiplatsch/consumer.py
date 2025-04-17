import json
import logging
from handle_client import HandleServiceClient


def create_consumer(topic: str, bootstrap_servers: str):
    """Creates and returns a consumer for the given topic."""
    from kafka import KafkaConsumer
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )


def process_message(message, handle_client):
    """Process a record message."""
    logging.info(f"Processing message: {message}")
    action = message.get("action")
    record = message.get("record")

    if action == "add":
        handle_client.add_pid(record)
    elif action == "update":
        handle_client.update_pid(record)
    elif action == "delete":
        handle_client.delete_pid(record.get("pid"))
    else:
        logging.warning(f"Unknown action {action} in message: {message}")


def consume(topic: str, bootstrap_servers: str, handle_client: HandleServiceClient):
    """Consume messages and process them."""
    consumer = create_consumer(topic, bootstrap_servers)
    
    logging.info(f"Starting consumer for topic '{topic}'...")

    for message in consumer:
        logging.info(f"Received message: {message.value}")
        process_message(message.value, handle_client)
