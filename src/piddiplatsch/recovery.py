import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from piddiplatsch.config import config
from piddiplatsch.handle_client import HandleClient

logger = logging.getLogger(__name__)


class FailureRecovery:
    FAILURE_DIR = (
        Path(config.get("consumer", {}).get("output_dir", "outputs")) / "failures"
    )
    FAILURE_DIR.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def record_failed_item(item: dict) -> None:
        """Append a failed STAC item to a daily JSONL file with UTC timestamp."""
        now = datetime.now(timezone.utc)
        timestamp = now.isoformat(timespec="seconds")
        dated_filename = f"failed_items_{now.date()}.jsonl"
        failure_file = FailureRecovery.FAILURE_DIR / dated_filename

        item_with_timestamp = {**item, "failure_timestamp": timestamp}

        with failure_file.open("a", encoding="utf-8") as f:
            json.dump(item_with_timestamp, f)
            f.write("\n")
        logger.debug(f"Recorded failed item to {failure_file}")

    @classmethod
    def retry_failed_items(
        cls,
        jsonl_path: Path,
        handle_client: Optional[HandleClient] = None,
        keep_failed: bool = False,
    ) -> tuple[int, int, Optional[Path]]:
        """
        Retry registering STAC items from a .jsonl failure log.

        Args:
            jsonl_path: Path to the JSONL file containing failed items.
            handle_client: Optional HandleClient instance.
            keep_failed: If True, save items that still fail to a new file.

        Returns:
            Tuple of (num_successful, num_failed, failed_output_path or None)
        """
        if handle_client is None:
            handle_client = HandleClient(config)

        success_count = 0
        fail_count = 0
        failed_out_path = None
        failed_out = None

        if keep_failed:
            now = datetime.now(timezone.utc)
            retry_suffix = now.strftime("%Y%m%dT%H%M%SZ")
            failed_out_path = jsonl_path.with_name(
                f"{jsonl_path.stem}_retry_failed_{retry_suffix}.jsonl"
            )
            failed_out = failed_out_path.open("a", encoding="utf-8")

        with jsonl_path.open("r", encoding="utf-8") as f:
            for line in f:
                try:
                    item = json.loads(line)
                    handle_client.register_item(item)
                    success_count += 1
                except Exception as e:
                    logger.error(f"Retry failed: {e}")
                    fail_count += 1
                    if failed_out:
                        failed_out.write(line)

        if failed_out:
            failed_out.close()

        return success_count, fail_count, failed_out_path
