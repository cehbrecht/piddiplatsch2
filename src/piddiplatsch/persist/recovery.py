import json
import logging
from datetime import UTC, datetime
from pathlib import Path

from piddiplatsch.config import config
from piddiplatsch.processing import RetryResult


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
    ) -> RetryResult:
        """
        Retry failed items from a JSONL file by reprocessing them through the pipeline.

        Returns a RetryResult with statistics about the retry operation.
        """
        from piddiplatsch.consumer import feed_messages_direct

        messages = FailureRecovery.load_failed_messages(jsonl_path)

        result = RetryResult(total=len(messages))

        if not messages:
            logging.warning("No messages to retry.")
            return result

        logging.info(f"Retrying {len(messages)} messages from {jsonl_path}...")

        # Track failure files before retry
        failure_files_before = set(FailureRecovery.FAILURE_DIR.rglob("*.jsonl"))

        # Process messages through pipeline
        feed_result = feed_messages_direct(
            messages, processor=processor, dry_run=dry_run
        )

        # Find new failure files created during retry
        failure_files_after = set(FailureRecovery.FAILURE_DIR.rglob("*.jsonl"))
        result.failure_files = failure_files_after - failure_files_before

        # Use stats from feed_result
        result.succeeded = feed_result.succeeded
        result.failed = feed_result.failed

        if delete_after and result.failed == 0:
            try:
                jsonl_path.unlink()
                logging.info(f"Deleted retry file: {jsonl_path}")
            except Exception as e:
                logging.warning(f"Could not delete {jsonl_path}: {e}")
        elif delete_after and result.failed > 0:
            logging.info(
                f"Skipping deletion of {jsonl_path} because {result.failed} items failed again"
            )

        return result

    @staticmethod
    def retry_batch(
        paths: tuple[Path, ...],
        processor: str,
        delete_after: bool = False,
        dry_run: bool = False,
    ) -> RetryResult:
        """
        Retry failed items from multiple files/directories.

        Resolves paths to JSONL files and processes them, aggregating results.

        Returns:
            RetryResult with overall statistics across all files.
        """
        files = FailureRecovery.find_retry_files(paths)

        if not files:
            logging.warning("No retry files found.")
            return RetryResult()

        logging.info(f"Found {len(files)} file(s) to retry.")

        overall = RetryResult()

        for file in files:
            result = FailureRecovery.retry(
                file, processor=processor, delete_after=delete_after, dry_run=dry_run
            )
            overall.total += result.total
            overall.succeeded += result.succeeded
            overall.failed += result.failed
            overall.failure_files.update(result.failure_files)

        return overall
