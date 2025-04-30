import os
import json
import uuid
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic


def get_producer(kafka_server):
    return Producer({"bootstrap.servers": kafka_server})


def get_admin_client(kafka_server):
    return AdminClient({"bootstrap.servers": kafka_server})


def ensure_topic_exists(kafka_server, topic):
    admin_client = get_admin_client(kafka_server)
    metadata = admin_client.list_topics(timeout=5)
    if topic not in metadata.topics:
        new_topic = NewTopic(topic, num_partitions=1, replication_factor=1)
        fs = admin_client.create_topics([new_topic])
        try:
            fs[topic].result()
        except Exception as e:
            raise RuntimeError(f"Failed to create topic '{topic}': {e}")


def send_message(kafka_server, topic, key, value, on_delivery=None):
    ensure_topic_exists(kafka_server, topic)
    producer = get_producer(kafka_server)
    producer.produce(
        topic,
        key=key.encode("utf-8"),
        value=value.encode("utf-8"),
        callback=on_delivery,
    )
    producer.flush()


def build_message_from_path(path):
    with open(path, "r") as f:
        data = json.load(f)
    key = os.path.splitext(os.path.basename(path))[0]
    value = json.dumps(data)
    return key, value


def build_message_from_json_string(message_str):
    data = json.loads(message_str)
    key = str(uuid.uuid5(uuid.NAMESPACE_DNS, message_str))
    value = json.dumps(data)
    return key, value
