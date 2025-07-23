import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer
from piddiplatsch.config import config

logger = logging.getLogger(__name__)


class FailureRecovery:
    FAILURE_DIR = (
        Path(config.get("consumer", {}).get("output_dir", "outputs")) / "failures"
    )
    FAILURE_DIR.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def record_failed_item(key: str, data: dict) -> None:
        """Append a failed STAC item to a daily JSONL file with UTC timestamp and retries=0."""
        now = datetime.now(timezone.utc)
        timestamp = now.isoformat(timespec="seconds")
        dated_filename = f"failed_items_{now.date()}.jsonl"
        failure_file = FailureRecovery.FAILURE_DIR / dated_filename

        data_with_metadata = {
            **data,
            "failure_timestamp": timestamp,
            "key": key,
            "retries": data.get("retries", 0),
        }

        with failure_file.open("a", encoding="utf-8") as f:
            json.dump(data_with_metadata, f)
            f.write("\n")

        logger.warning(f"Recorded failed item {key} to {failure_file}")

    @staticmethod
    def retry(
        jsonl_path: Path,
        retry_topic: str,
        delete_after: bool = False,
    ):
        """Retry failed items by sending them to a Kafka retry topic, incrementing 'retries'."""
        kafka_config = config.get("kafka", {})
        bootstrap_servers = kafka_config.get("bootstrap.servers", "localhost:9092")

        if not jsonl_path.exists():
            logger.error(f"Retry file not found: {jsonl_path}")
            return 0, 0

        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
        )

        success = 0
        failed = 0

        with jsonl_path.open("r", encoding="utf-8") as f:
            for line in f:
                try:
                    record = json.loads(line)
                    key = record.get("key", "unknown")
                    record["retries"] = int(record.get("retries", 0)) + 1
                    producer.send(retry_topic, key=key, value=record)
                    success += 1
                except Exception as e:
                    logger.error(f"Failed to send retry message: {e}")
                    failed += 1

        producer.flush()

        if delete_after and success > 0 and failed == 0:
            jsonl_path.unlink()
            logger.info(f"Deleted successfully retried file: {jsonl_path}")

        logger.info(f"Retried {success} messages, {failed} failed from {jsonl_path}")
        return success, failed
