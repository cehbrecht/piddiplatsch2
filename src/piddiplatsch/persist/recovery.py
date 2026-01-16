import json
import logging
from datetime import UTC, datetime
from pathlib import Path

from piddiplatsch.config import config


class FailureRecovery:
    FAILURE_DIR = (
        Path(config.get("consumer", {}).get("output_dir", "outputs")) / "failures"
    )
    FAILURE_DIR.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def record_failed_item(
        key: str, data: dict, retries: int = 0, reason: str = "Unknown"
    ) -> None:
        """Append a failed STAC item to a daily JSONL file with UTC timestamp, under retries-N folder."""
        now = datetime.now(UTC)
        timestamp = now.isoformat(timespec="seconds")
        dated_filename = f"failed_items_{now.date()}.jsonl"

        retry_folder = FailureRecovery.FAILURE_DIR / f"r{retries}"
        retry_folder.mkdir(parents=True, exist_ok=True)

        failure_file = retry_folder / dated_filename

        infos = {
            "failure_timestamp": timestamp,
            "retries": retries,
            "reason": reason,
        }

        data_with_metadata = {
            **data,
            "__infos__": infos,
        }

        with failure_file.open("a", encoding="utf-8") as f:
            json.dump(data_with_metadata, f)
            f.write("\n")

        logging.warning(
            f"Recorded failed item {key} (retries={retries}) to {failure_file}"
        )

    @staticmethod
    def load_failed_messages(jsonl_path: Path) -> list[tuple[str, dict]]:
        """Load failed items from a JSONL file and return as (key, value) tuples for DirectConsumer."""
        if not jsonl_path.exists():
            logging.error(f"Retry file not found: {jsonl_path}")
            return []

        messages = []
        with jsonl_path.open("r", encoding="utf-8") as f:
            for line in f:
                try:
                    record = json.loads(line)
                    key = str(record.get("key") or record.get("id") or "unknown")

                    # Increment retries counter
                    if "__infos__" in record:
                        record["__infos__"]["retries"] = (
                            int(record["__infos__"].get("retries", 0)) + 1
                        )
                    else:
                        record["retries"] = int(record.get("retries", 0)) + 1

                    messages.append((key, record))
                except Exception:
                    logging.exception(
                        f"Error loading failed item from {jsonl_path}: {line.strip()}"
                    )

        logging.info(f"Loaded {len(messages)} messages from {jsonl_path}")
        return messages

    @staticmethod
    def find_retry_files(paths: tuple[Path, ...]) -> list[Path]:
        """
        Find all JSONL files from the given paths.

        Supports:
        - Individual files
        - Directories (finds all .jsonl files)
        - Glob patterns

        Returns sorted list of unique file paths.
        """
        files = set()

        for path in paths:
            if path.is_file():
                if path.suffix == ".jsonl":
                    files.add(path)
                else:
                    logging.warning(f"Skipping non-JSONL file: {path}")
            elif path.is_dir():
                # Find all .jsonl files in directory (non-recursive)
                jsonl_files = path.glob("*.jsonl")
                files.update(jsonl_files)
            else:
                # Treat as glob pattern
                parent = path.parent
                pattern = path.name
                matched = parent.glob(pattern)
                files.update(f for f in matched if f.is_file() and f.suffix == ".jsonl")

        return sorted(files)

    @staticmethod
    def retry(
        jsonl_path: Path,
        processor: str,
        delete_after: bool = False,
        dry_run: bool = False,
    ) -> int:
        """
        Retry failed items from a JSONL file by reprocessing them through the pipeline.

        Returns the number of messages retried.
        """
        from piddiplatsch.consumer import feed_messages_direct

        messages = FailureRecovery.load_failed_messages(jsonl_path)

        if not messages:
            logging.warning("No messages to retry.")
            return 0

        logging.info(f"Retrying {len(messages)} messages from {jsonl_path}...")
        feed_messages_direct(messages, processor=processor, dry_run=dry_run)

        if delete_after:
            try:
                jsonl_path.unlink()
                logging.info(f"Deleted retry file: {jsonl_path}")
            except Exception as e:
                logging.warning(f"Could not delete {jsonl_path}: {e}")

        return len(messages)
