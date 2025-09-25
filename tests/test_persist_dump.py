# tests/test_dump.py
import json
from datetime import UTC, datetime

import pytest

from piddiplatsch.persist.dump import DumpRecorder


@pytest.fixture
def temp_dump_dir(tmp_path, monkeypatch):
    """
    Provide a temporary dump directory and patch DumpRecorder.DUMP_DIR.
    Ensures tests do not affect the real filesystem.
    """
    dump_dir = tmp_path / "dump"
    dump_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(DumpRecorder, "DUMP_DIR", dump_dir)
    return dump_dir


def test_record_single_item(temp_dump_dir):
    """Test that a single record is correctly written to a JSONL file."""
    key = "testkey"
    data = {"foo": "bar"}

    DumpRecorder.record_item(key, data)

    files = list(temp_dump_dir.glob("*.jsonl"))
    assert len(files) == 1

    with files[0].open("r", encoding="utf-8") as f:
        lines = f.readlines()

    assert len(lines) == 1
    assert json.loads(lines[0]) == data


def test_record_multiple_items_append(temp_dump_dir):
    """Test that multiple records append to the same JSONL file."""
    records = [
        ("key1", {"foo": "bar"}),
        ("key2", {"baz": 123}),
        ("key3", {"nested": {"a": 1}}),
    ]

    for key, data in records:
        DumpRecorder.record_item(key, data)

    files = list(temp_dump_dir.glob("*.jsonl"))
    assert len(files) == 1
    dump_file = files[0]

    with dump_file.open("r", encoding="utf-8") as f:
        lines = f.readlines()

    assert len(lines) == len(records)
    for line, (_, data) in zip(lines, records, strict=False):
        assert json.loads(line) == data


def test_dump_filename_contains_today(temp_dump_dir):
    """Test that the dump filename includes today's date."""
    key = "datekey"
    data = {"hello": "world"}

    DumpRecorder.record_item(key, data)

    files = list(temp_dump_dir.glob("*.jsonl"))
    assert len(files) == 1

    today = datetime.now(UTC).date()
    expected_filename = f"dump_messages_{today}.jsonl"
    actual_filename = files[0].name

    assert actual_filename == expected_filename
