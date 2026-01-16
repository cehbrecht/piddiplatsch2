import json
import logging
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from piddiplatsch.config import config


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


# ---------------------------------------------------------
# Test convenience helpers (used by smoke/integration tests)
# ---------------------------------------------------------


def ensure_topic_exists_from_config():
    """Ensure the configured Kafka topic exists (smoke tests)."""
    topic = config.get("consumer", "topic")
    kafka_cfg = config.get("kafka")
    ensure_topic_exists(topic, kafka_cfg)


def send_message_to_kafka(json_path: Path):
    """Send a JSON message (from file) to Kafka using config defaults."""
    key, value = build_message_from_path(json_path)

    logging.info(f"[smoke] sending file={json_path.name} as key={key}")

    def _report(err, msg):
        if err:
            logging.error(f"[smoke] delivery failed: {err}")
        else:
            logging.info(f"[smoke] delivered to {msg.topic()}[{msg.partition()}]")

    topic = config.get("consumer", "topic")
    kafka_cfg = config.get("kafka")
    send_message(topic, kafka_cfg, key, value, on_delivery=_report)
