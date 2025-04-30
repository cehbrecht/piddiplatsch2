import logging
import json
import uuid
from confluent_kafka import Consumer as ConfluentConsumer, KafkaException
from piddiplatsch.handle_client import HandleClient
from piddiplatsch.config import config

# Load Handle Service configuration
HANDLE_SERVER_URL = config.get("handle", "server_url")
HANDLE_PREFIX = config.get("handle", "prefix")
USERNAME = config.get("handle", "username")
PASSWORD = config.get("handle", "password")

# Configure logging
config.configure_logging()


def build_client():
    return HandleClient(
        server_url=HANDLE_SERVER_URL,
        prefix=HANDLE_PREFIX,
        username=USERNAME,
        password=PASSWORD,
    )


class Consumer:
    def __init__(
        self, topic: str, kafka_server: str, group_id: str = "piddiplatsch-consumer"
    ):
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
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())
                key = msg.key().decode("utf-8")
                logging.debug(f"Got a message: {key}")
                value = json.loads(msg.value().decode("utf-8"))
                yield key, value
        finally:
            self.consumer.close()


def start_consumer(topic, kafka_server):
    logging.info(f"Starting Kafka consumer for topic: {topic}")
    consumer = Consumer(topic, kafka_server)
    for key, value in consumer.consume():
        process_message(key, value)


def process_message(key, value):
    logging.info(f"Processing message: {key}")
    pid = build_pid(key, value)
    record = build_record(value)
    add_item(pid, record)


def build_pid(key, value):
    try:
        pid = value["data"]["payload"]["item"]["id"]
    except Exception:
        pid = str(uuid.uuid5(uuid.NAMESPACE_DNS, key))
    return pid


def build_record(value):
    url = value["data"]["payload"]["item"]["links"][0]["href"]
    return {"URL": url, "CHECKSUM": None}


def add_item(pid, record):
    logging.info(f"add item: pid = {pid}, record = {record}")
    handle_client = build_client()
    try:
        handle_client.add_item(pid, record)
        logging.info(f"Added item: pid = {pid}")
    except Exception as e:
        logging.error(f"Failed to add item with pid = {pid}: {e}")
