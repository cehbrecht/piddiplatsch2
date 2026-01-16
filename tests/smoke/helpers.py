"""Utility helpers for smoke tests only (Kafka topic & sending).

These helpers replicate the previous CLI-only commands `init` and `send`
so smoke tests can create topics and push messages when needed.
"""

from pathlib import Path
import json

from piddiplatsch.config import config
from piddiplatsch.testing import kafka_client


def ensure_kafka_topic_exists():
    """Create Kafka topic configured in `consumer.topic` if missing.

    Intended for smoke tests when using Kafka-backed flows.
    """
    topic = config.get("consumer", "topic")
    kafka_cfg = config.get("kafka")
    kafka_client.ensure_topic_exists(topic, kafka_cfg)


def send_json_file_to_kafka(json_path: Path, *, verbose: bool = False):
    """Send a JSON file as a message to the configured Kafka topic.

    - `json_path`: path to a JSON input file used to build the message key/value
    - `verbose`: when True, prints the key and payload size
    """
    key, value = kafka_client.build_message_from_path(json_path)

    if verbose:
        try:
            obj = json.loads(value)
            payload_info = f"{len(value)} bytes, keys: {', '.join(obj.keys())}"
        except Exception:
            payload_info = f"{len(value)} bytes"
        print(f"[smoke] key={key}, payload={payload_info}")

    def _report(err, msg):
        if err:
            print(f"[smoke] delivery failed: {err}")
        else:
            print(f"[smoke] delivered to {msg.topic()}[{msg.partition()}]")

    topic = config.get("consumer", "topic")
    kafka_cfg = config.get("kafka")
    kafka_client.send_message(topic, kafka_cfg, key, value, on_delivery=_report)
