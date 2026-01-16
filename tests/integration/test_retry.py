"""Integration tests for retry functionality."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from piddiplatsch.config import config
from piddiplatsch.persist.recovery import FailureRecovery

pytestmark = pytest.mark.integration


def _make_minimal_cmip6_item() -> dict:
    """Create a minimal valid CMIP6 item for testing."""
    ds_id = "CMIP6.ActivityX.InstitutionY.SourceZ.ssp245.r1i1p1f1.Amon.pr.gn.v20190101"
    return {
        "id": ds_id,
        "type": "Feature",
        "collection": "cmip6",
        "properties": {},
        "assets": {
            "data0000": {
                "alternate:name": "ceda.ac.uk",
                "published_on": "2019-01-01 00:00:00",
            }
        },
    }


def _create_failure_jsonl(failure_file: Path, num_items: int = 3) -> None:
    """Create a failure JSONL file with test items."""
    failure_file.parent.mkdir(parents=True, exist_ok=True)

    with failure_file.open("w", encoding="utf-8") as f:
        for i in range(num_items):
            item = _make_minimal_cmip6_item()
            # Vary the id slightly
            item["id"] = item["id"].replace("pr.gn", f"pr{i}.gn")

            # Wrap in the format that would be saved by record_failed_item
            data = {
                "data": {
                    "payload": {"item": item},
                },
                "metadata": {"time": datetime.now(UTC).isoformat()},
                "key": f"test-key-{i}",
                "__infos__": {
                    "failure_timestamp": datetime.now(UTC).isoformat(),
                    "retries": 0,
                    "reason": "Test failure",
                },
            }
            json.dump(data, f)
            f.write("\n")


def test_retry_loads_and_processes_failed_messages(tmp_path: Path):
    """Test that retry() loads failed messages and processes them through the pipeline."""
    # Configure to use temp directory and dry-run mode
    config._set("consumer", "output_dir", str(tmp_path))
    config._set("lookup", "enabled", False)

    # Create a failure file
    failure_file = tmp_path / "failures" / "r0" / "failed_items_2026-01-16.jsonl"
    _create_failure_jsonl(failure_file, num_items=3)

    # Verify file exists and has 3 items
    assert failure_file.exists()
    with failure_file.open("r") as f:
        lines = f.readlines()
    assert len(lines) == 3

    # Retry the failed messages
    count = FailureRecovery.retry(
        failure_file, processor="cmip6", delete_after=False, dry_run=True
    )

    # Should have retried 3 messages
    assert count == 3

    # File should still exist (delete_after=False)
    assert failure_file.exists()

    # Verify handles were created in dry-run mode
    handles_dir = tmp_path / "handles"
    assert handles_dir.exists()

    files = list(handles_dir.glob("handles_*.jsonl"))
    assert files, "No handles jsonl file created after retry"

    # Should have at least 3 lines (one per dataset, possibly more with assets)
    with files[0].open("r", encoding="utf-8") as f:
        content_lines = [line for line in f if line.strip()]
    assert len(content_lines) >= 3


def test_retry_increments_retry_counter(tmp_path: Path):
    """Test that retry() increments the retries counter in __infos__."""
    config._set("consumer", "output_dir", str(tmp_path))
    config._set("lookup", "enabled", False)

    # Create a failure file with retries=0
    failure_file = tmp_path / "failures" / "r0" / "failed_items_2026-01-16.jsonl"
    _create_failure_jsonl(failure_file, num_items=1)

    # Load messages to check retry counter
    messages = FailureRecovery.load_failed_messages(failure_file)
    assert len(messages) == 1

    _key, data = messages[0]
    assert "__infos__" in data
    assert data["__infos__"]["retries"] == 1  # Should be incremented from 0 to 1


def test_retry_deletes_file_when_delete_after_true(tmp_path: Path):
    """Test that retry() deletes the file when delete_after=True."""
    config._set("consumer", "output_dir", str(tmp_path))
    config._set("lookup", "enabled", False)

    # Create a failure file
    failure_file = tmp_path / "failures" / "r0" / "failed_items_2026-01-16.jsonl"
    _create_failure_jsonl(failure_file, num_items=2)

    assert failure_file.exists()

    # Retry with delete_after=True
    count = FailureRecovery.retry(
        failure_file, processor="cmip6", delete_after=True, dry_run=True
    )

    assert count == 2
    # File should be deleted
    assert not failure_file.exists()


def test_retry_handles_nonexistent_file(tmp_path: Path):
    """Test that retry() handles nonexistent file gracefully."""
    config._set("consumer", "output_dir", str(tmp_path))

    nonexistent_file = tmp_path / "does_not_exist.jsonl"

    # Should return 0 and not raise an exception
    count = FailureRecovery.retry(nonexistent_file, processor="cmip6")
    assert count == 0


def test_retry_handles_empty_file(tmp_path: Path):
    """Test that retry() handles empty file gracefully."""
    config._set("consumer", "output_dir", str(tmp_path))

    empty_file = tmp_path / "empty.jsonl"
    empty_file.touch()

    count = FailureRecovery.retry(empty_file, processor="cmip6")
    assert count == 0
