import logging
import json
from confluent_kafka import Consumer as ConfluentConsumer, KafkaException
from piddiplatsch.plugin import pm
from piddiplatsch.config import config

# Configure logging (console or file with optional colors)
config.configure_logging()


def load_processor():
    """Load the processor plugin specified in the config."""
    processor_class = config.get("processor", "class")
    if not processor_class:
        logging.error("No processor class specified in the config.")
        return None

    # Dynamically load and register the processor plugin
    plugin = pm.get_plugin(processor_class)
    if not plugin:
        logging.error(f"Processor plugin '{processor_class}' not found.")
        return None

    return plugin


class Consumer:
    def __init__(
        self, topic: str, kafka_server: str, group_id: str = "piddiplatsch-consumer"
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
    """Start the Kafka consumer and process messages."""
    logging.info(f"Starting Kafka consumer for topic: {topic}")

    # Load the appropriate processor plugin from config
    plugin = load_processor()
    if plugin is None:
        logging.error("No valid processor found. Exiting consumer.")
        return

    processor = plugin()

    # Start the consumer and process messages
    consumer = Consumer(topic, kafka_server)
    for key, value in consumer.consume():
        if processor.can_process(key, value):
            processor.process(
                key, value, handle_client
            )  # Pass the handle_client to the process method
