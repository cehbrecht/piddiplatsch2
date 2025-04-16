import json
import logging
from kafka import KafkaConsumer
from functools import lru_cache
import pyhandle

# Handle Service Configuration
HANDLE_SERVER_URL = "http://localhost:5000/"  # Mock server for testing
HANDLE_PREFIX = "21.T11148"

# Initialize Handle Client
client = pyhandle.handleclient.PyHandleClient("rest")
client.instantiate_with_username_and_password(
    handle_server_url=HANDLE_SERVER_URL,
    username="300:foo/bar",
    password="mypassword",
    HTTPS_verify=False,  # optional for HTTP or self-signed certs
)
client.handle_client._RESTHandleClient__handlesystemconnector._HandleSystemConnector__has_write_access = (
    True
)


def add_pid(record):
    """Adds a PID to the Handle Service."""
    if "pid" not in record:
        raise ValueError("Record must contain a 'pid' field.")

    pid = f"{HANDLE_PREFIX}/{record['pid']}"

    try:
        client.register_handle(pid, record)
        logging.info(f"Added PID {pid} for record: {record}")
    except Exception as e:
        logging.error(f"Failed to register PID {pid}: {e}")
        raise


def update_pid(record):
    """Updates a PID in the Handle Service."""
    pid = record.get("pid")
    if pid:
        client.modify_handle(pid, record)
        logging.info(f"Updated PID {pid} for record: {record}")


def delete_pid(pid):
    """Deletes a PID from the Handle Service."""
    client.delete_handle(pid)
    logging.info(f"Deleted PID: {pid}")


@lru_cache(maxsize=1000)
def lookup_pid(identifier):
    """Searches for an existing PID in the Handle Service (cached)."""
    pid = f"{HANDLE_PREFIX}/{identifier}"
    try:
        handle_data = client.retrieve_handle_record(pid)
        logging.info(f"Found PID {pid}: {handle_data}")
        return handle_data
    except Exception:
        logging.info(f"PID {pid} not found.")
        return None


def create_consumer(topic: str, bootstrap_servers: str):
    """Creates and returns a Kafka consumer for CMIP7 records."""
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )


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
