import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer
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
        retry_topic: str,
        kafka_cfg: dict,
        jsonl_path: Path,
        delete_after: bool = False,
    ) -> tuple[int, int]:
        """Retry failed items from a JSONL file by sending them to Kafka using confluent_kafka."""
        if not jsonl_path.exists():
            logging.error(f"Retry file not found: {jsonl_path}")
            return 0, 0

        producer = Producer({k: str(v) for k, v in kafka_cfg.items()})

        success, failed = 0, 0

        def delivery_report(err, msg):
            nonlocal success, failed
            if err is not None:
                logging.error(f"Delivery failed for {msg.key()}: {err}")
                failed += 1
            else:
                logging.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")
                success += 1

        with jsonl_path.open("r", encoding="utf-8") as f:
            for line in f:
                try:
                    record = json.loads(line)
                    key = str(record.get("key") or record.get("id") or "unknown")
                    record["retries"] = int(record.get("retries", 0)) + 1
                    value = json.dumps(record).encode("utf-8")
                    producer.produce(
                        topic=retry_topic,
                        key=key.encode("utf-8"),
                        value=value,
                        callback=delivery_report,
                    )
                except Exception as e:
                    logging.exception(f"Error retrying failed item: {e}")
                    failed += 1

        producer.flush()

        if delete_after and success > 0 and failed == 0:
            try:
                jsonl_path.unlink()
                logging.info(f"Deleted retried file: {jsonl_path}")
            except Exception as e:
                logging.warning(f"Could not delete {jsonl_path}: {e}")

        return success, failed
