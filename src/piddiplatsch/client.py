import json
import logging
import uuid
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic


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
