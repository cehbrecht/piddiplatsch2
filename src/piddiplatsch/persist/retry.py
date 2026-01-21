import logging
from pathlib import Path
from collections.abc import Callable

from piddiplatsch.persist.helpers import find_jsonl, read_jsonl
from piddiplatsch.processing import RetryResult


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
            record["__infos__"]["retries"] = int(record["__infos__"].get("retries", 0)) + 1
        else:
            record["retries"] = int(record.get("retries", 0)) + 1
        messages.append((key, record))

    logging.info(f"Loaded {len(messages)} messages from {jsonl_path}")
    return messages


def find_retry_files(paths: tuple[Path, ...]) -> list[Path]:
    """
    Find all JSONL files from the given paths.

    Supports files, directories, and glob patterns. Returns sorted unique paths.
    """
    return find_jsonl(paths)


def retry(
    jsonl_path: Path,
    processor: str,
    *,
    failure_dir: Path,
    delete_after: bool = False,
    dry_run: bool = False,
) -> RetryResult:
    """Retry failed items from a JSONL file by reprocessing them through the pipeline."""
    from piddiplatsch.consumer import feed_messages_direct

    messages = load_failed_messages(jsonl_path)

    result = RetryResult(total=len(messages))
    if not messages:
        logging.warning("No messages to retry.")
        return result

    logging.info(f"Retrying {len(messages)} messages from {jsonl_path}...")

    # Track failure files before retry
    failure_files_before = set(failure_dir.rglob("*.jsonl"))

    # Process messages through pipeline
    feed_result = feed_messages_direct(messages, processor=processor, dry_run=dry_run)

    # Find new failure files created during retry
    failure_files_after = set(failure_dir.rglob("*.jsonl"))
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


def retry_batch(
    paths: tuple[Path, ...],
    processor: str,
    *,
    failure_dir: Path,
    delete_after: bool = False,
    dry_run: bool = False,
    verbose: bool = False,
    progress_callback: Callable[[Path, int, int, RetryResult], None] | None = None,
) -> RetryResult:
    """Retry failed items from multiple files/directories and aggregate results."""
    files = find_retry_files(paths)

    if not files:
        logging.warning("No retry files found.")
        return RetryResult()

    logging.info(f"Found {len(files)} file(s) to retry.")

    overall = RetryResult()
    total_files = len(files)

    for idx, file in enumerate(files, 1):
        result = retry(
            file,
            processor=processor,
            failure_dir=failure_dir,
            delete_after=delete_after,
            dry_run=dry_run,
        )
        overall.total += result.total
        overall.succeeded += result.succeeded
        overall.failed += result.failed
        overall.failure_files.update(result.failure_files)

        if progress_callback:
            progress_callback(file, idx, total_files, result)

    return overall
