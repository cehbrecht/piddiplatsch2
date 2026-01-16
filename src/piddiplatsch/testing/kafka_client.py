import json
import logging
import time
import uuid
from pathlib import Path

from confluent_kafka import Consumer as ConfluentConsumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from piddiplatsch.config import config
from piddiplatsch.consumer import feed_messages_direct


def client_cfg(kafka_cfg):
    cfg = {"bootstrap.servers": kafka_cfg["bootstrap.servers"]}
    return cfg


def get_producer(kafka_cfg):
    return Producer(client_cfg(kafka_cfg))


def get_admin_client(kafka_cfg):
    return AdminClient(client_cfg(kafka_cfg))


def ensure_topic_exists(topic, kafka_cfg, num_partitions=1, replication_factor=1):
    admin_client = get_admin_client(kafka_cfg)
    metadata = admin_client.list_topics(timeout=5)

    if topic in metadata.topics:
        logging.debug(f"Kafka topic '{topic}' already exists.")
        return

    new_topic = NewTopic(
        topic, num_partitions=num_partitions, replication_factor=replication_factor
    )
    futures = admin_client.create_topics([new_topic])

    try:
        futures[topic].result()
        logging.info(f"Kafka topic '{topic}' created.")
    except Exception as e:
        # Idempotency: ignore 'already exists' errors which may occur in races
        msg = str(e)
        if "TOPIC_ALREADY_EXISTS" in msg or "already exists" in msg:
            logging.debug(f"Kafka topic '{topic}' already exists (ignored).")
            return
        logging.error(f"Failed to create topic '{topic}': {e}")
        raise


def send_message(topic, kafka_cfg, key, value, on_delivery=None):
    ensure_topic_exists(topic, kafka_cfg)
    producer = get_producer(kafka_cfg)
    producer.produce(
        topic,
        key=key.encode("utf-8"),
        value=value.encode("utf-8"),
        callback=on_delivery,
    )
    producer.flush()


def build_message_from_path(path):
    path = Path(path)
    with path.open() as f:
        data = json.load(f)
    key = path.stem
    value = json.dumps(data)
    return key, value


def build_message_from_json_string(message_str):
    data = json.loads(message_str)
    key = str(uuid.uuid5(uuid.NAMESPACE_DNS, message_str))
    value = json.dumps(data)
    return key, value


# ---------------------------------------------------------
# Test convenience helpers (used by smoke/integration tests)
# ---------------------------------------------------------


def ensure_topic_exists_from_config():
    """Ensure the configured Kafka topic exists (smoke tests)."""
    topic = config.get("consumer", "topic")
    kafka_cfg = config.get("kafka")
    ensure_topic_exists(topic, kafka_cfg)


def send_json_file_to_kafka_from_config(json_path: Path, *, verbose: bool = False):
    """Send a JSON file to Kafka using config defaults (smoke tests)."""
    key, value = build_message_from_path(json_path)

    if verbose:
        try:
            obj = json.loads(value)
            info = f"{len(value)} bytes, keys: {', '.join(obj.keys())}"
        except Exception:
            info = f"{len(value)} bytes"
        logging.info(f"[smoke] key={key}, payload={info}")

    def _report(err, msg):
        if err:
            logging.error(f"[smoke] delivery failed: {err}")
        else:
            logging.info(f"[smoke] delivered to {msg.topic()}[{msg.partition()}]")

    topic = config.get("consumer", "topic")
    kafka_cfg = config.get("kafka")
    send_message(topic, kafka_cfg, key, value, on_delivery=_report)


def _poll_kafka_messages(idle_timeout: float = 1.0, max_messages: int | None = None):
    """Poll configured Kafka topic and return a list of (key, value) tuples.

    Stops when no messages are received for `idle_timeout` seconds,
    or when `max_messages` is reached.
    """
    topic = config.get("consumer", "topic")
    kafka_cfg = config.get("kafka")
    consumer = ConfluentConsumer(kafka_cfg)
    consumer.subscribe([topic])

    messages = []
    idle_since = None
    try:
        while True:
            msg = consumer.poll(timeout=0.5)
            if msg is None:
                if idle_since is None:
                    idle_since = time.time()
                elif time.time() - idle_since >= idle_timeout:
                    break
                continue

            idle_since = None

            if msg.error():
                logging.error(f"Kafka error: {msg.error()}")
                continue

            key = msg.key().decode("utf-8") if msg.key() else None
            try:
                value = json.loads(msg.value().decode("utf-8"))
            except json.JSONDecodeError as e:
                logging.error(f"Failed to decode message: {e}")
                continue

            messages.append((key, value))

            if max_messages is not None and len(messages) >= max_messages:
                break
    finally:
        consumer.close()

    return messages


def consume_available_messages(processor: str = "cmip6", *, idle_timeout: float = 1.0, max_messages: int | None = None):
    """Consume available Kafka messages and process them once via pipeline.

    Pulls messages from Kafka, then feeds them into the processing pipeline
    using the direct consumer. Intended for smoke tests where we want to
    exercise Kafka ingestion but keep the test run bounded.
    """
    messages = _poll_kafka_messages(idle_timeout=idle_timeout, max_messages=max_messages)
    if messages:
        feed_messages_direct(messages, processor=processor)
    return len(messages)
