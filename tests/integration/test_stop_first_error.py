from pathlib import Path

import pytest

from piddiplatsch.config import config
from piddiplatsch.consumer import start_consumer
from piddiplatsch.core.processing import BaseProcessor
from piddiplatsch.result import ProcessingResult


class BoomProcessor(BaseProcessor):
    def preflight_check(self, **kwargs):
        return None

    def process(self, key, value):
        if key == "bad1":
            raise RuntimeError("boom: simulated processing error")
        return ProcessingResult(key=key, success=True, num_handles=0)


class OKProcessor(BaseProcessor):
    def preflight_check(self, **kwargs):
        return None

    def process(self, key, value):
        return ProcessingResult(key=key, success=True, num_handles=0)


def _clean_failures():
    out = Path("outputs/failures")
    out.mkdir(parents=True, exist_ok=True)
    for p in out.glob("**/*.jsonl"):
        try:
            p.unlink()
        except Exception:
            pass


def test_stop_on_first_error(tmp_path):
    _clean_failures()
    # Ensure conservative setting for this test
    consumer_cfg = dict(config.get("consumer", {}))
    consumer_cfg["max_errors"] = 1
    config._set("consumer", None, consumer_cfg)
    with pytest.raises(SystemExit) as exc:
        start_consumer(
            processor=BoomProcessor(dry_run=True),
            direct_messages=[("bad1", {"retries": 0}), ("good1", {"ok": True})],
            dry_run=True,
            verbose=False,
        )
    assert exc.value.code == 1

    # Failure JSONL should be created
    failure_files = sorted(Path("outputs/failures").glob("**/*.jsonl"))
    assert len(failure_files) >= 1


def test_proceed_after_fix(tmp_path):
    _clean_failures()
    # After fixing, consumer should process both messages successfully
    start_consumer(
        processor=OKProcessor(dry_run=True),
        direct_messages=[("bad1", {"fixed": True}), ("good2", {"ok": True})],
        dry_run=True,
        verbose=False,
    )
