import logging
from datetime import UTC, datetime
from pathlib import Path

from piddiplatsch.config import config
from piddiplatsch.persist.base import (
    RecorderBase,
    find_jsonl,
    read_jsonl,
)
from piddiplatsch.processing import RetryResult


class FailureRecovery(RecorderBase):
    FAILURE_DIR = (
        Path(config.get("consumer", {}).get("output_dir", "outputs")) / "failures"
    )
    FAILURE_DIR.mkdir(parents=True, exist_ok=True)

    def __init__(self) -> None:
        super().__init__(self.FAILURE_DIR, "failed_items")

    def prepare(
        self,
        key: str,
        data: dict,
        reason: str | None,
        retries: int | None,
    ) -> tuple[dict, dict | None, Path | None]:
        ts = datetime.now(UTC).isoformat(timespec="seconds")
        r = 0 if retries is None else int(retries)
        infos = {"failure_timestamp": ts, "retries": r, "reason": reason or "Unknown"}
        subdir = self.root_dir / f"r{r}"
        return data, infos, subdir

    @staticmethod
    def record(
        key: str, data: dict, *, reason: str | None = None, retries: int | None = None
    ) -> None:
        path = FailureRecovery().write(key, data, reason=reason, retries=retries)
        logging.warning(
            f"Recorded failed item {key} (retries={0 if retries is None else retries}) to {path}"
        )

    @staticmethod
    def load_failed_messages(jsonl_path: Path) -> list[tuple[str, dict]]:
        """Load failed (or skipped) items from JSONL and return as (key, value) tuples."""
        records = read_jsonl(jsonl_path)
        if not records:
            logging.error(f"Retry file not found or empty: {jsonl_path}")
            return []

        messages: list[tuple[str, dict]] = []
        for record in records:
            key = str(record.get("key") or record.get("id") or "unknown")
            if "__infos__" in record:
                record["__infos__"]["retries"] = (
                    int(record["__infos__"].get("retries", 0)) + 1
                )
            else:
                record["retries"] = int(record.get("retries", 0)) + 1
            messages.append((key, record))

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
        return find_jsonl(paths)

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
        verbose: bool = False,
        progress_callback=None,
    ) -> RetryResult:
        """
        Retry failed items from multiple files/directories.

        Resolves paths to JSONL files and processes them, aggregating results.

        Args:
            paths: File/directory paths to retry
            processor: Processor name
            delete_after: Delete files after successful retry
            dry_run: Run without contacting Handle Service
            verbose: Show progress information
            progress_callback: Optional callback for progress updates (file, index, total, result)

        Returns:
            RetryResult with overall statistics across all files.
        """
        files = FailureRecovery.find_retry_files(paths)

        if not files:
            logging.warning("No retry files found.")
            return RetryResult()

        logging.info(f"Found {len(files)} file(s) to retry.")

        overall = RetryResult()
        total_files = len(files)

        for idx, file in enumerate(files, 1):
            result = FailureRecovery.retry(
                file, processor=processor, delete_after=delete_after, dry_run=dry_run
            )
            overall.total += result.total
            overall.succeeded += result.succeeded
            overall.failed += result.failed
            overall.failure_files.update(result.failure_files)

            # Call progress callback if provided (for CLI progress display)
            if progress_callback:
                progress_callback(file, idx, total_files, result)

        return overall
