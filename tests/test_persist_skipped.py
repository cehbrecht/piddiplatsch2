import json
from datetime import UTC, datetime

import pytest

from piddiplatsch.persist.skipped import SkipRecorder


@pytest.fixture
def temp_skipped_dir(tmp_path, monkeypatch):
    """
    Provide a temporary skipped directory and patch SkipRecorder.SKIPPED_DIR.
    Ensures tests do not affect the real filesystem.
    """
    skipped_dir = tmp_path / "skipped"
    skipped_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(SkipRecorder, "SKIPPED_DIR", skipped_dir)
    return skipped_dir


def test_record_single_skipped_item(temp_skipped_dir):
    key = "testkey"
    data = {"foo": "bar"}

    SkipRecorder.record(key, data, reason="network failure")

    files = list(temp_skipped_dir.glob("*.jsonl"))
    assert len(files) == 1

    with files[0].open("r", encoding="utf-8") as f:
        lines = f.readlines()

    assert len(lines) == 1
    obj = json.loads(lines[0])
    assert obj["foo"] == "bar"
    assert "__infos__" in obj
    assert obj["__infos__"]["reason"] == "network failure"
    assert isinstance(obj["__infos__"]["retries"], int)


def test_skipped_filename_contains_today(temp_skipped_dir):
    key = "datekey"
    data = {"hello": "world"}

    SkipRecorder.record(key, data, reason="timeout")

    files = list(temp_skipped_dir.glob("*.jsonl"))
    assert len(files) == 1

    today = datetime.now(UTC).date()
    expected_filename = f"skipped_items_{today}.jsonl"
    actual_filename = files[0].name

    assert actual_filename == expected_filename
