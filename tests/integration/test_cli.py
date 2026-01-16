import json
from datetime import UTC, datetime

import pytest

from piddiplatsch.cli import cli
from piddiplatsch.config import config

pytestmark = pytest.mark.integration


@pytest.mark.skip(reason="not used")
def test_send_invalid_path(runner):
    result = runner.invoke(cli, ["send", "nonexistent.json"])
    assert result.exit_code == 2
    assert "Usage: cli send [OPTIONS] FILENAME" in result.output


def test_retry_with_dry_run(runner, tmp_path):
    """Test that retry command works with --dry-run flag."""
    config._set("consumer", "output_dir", str(tmp_path))
    config._set("lookup", "enabled", False)

    # Create a failure file with a valid CMIP6 ID
    failure_file = tmp_path / "test_failure.jsonl"
    item = {
        "id": "CMIP6.CMIP.MOHC.HadGEM3-GC31-LL.ssp245.r1i1p1f1.Amon.pr.gn.v20190101",
        "type": "Feature",
        "collection": "cmip6",
        "properties": {},
        "assets": {
            "data0000": {
                "alternate:name": "ceda.ac.uk",
                "published_on": "2026-01-16 00:00:00",
            }
        },
    }

    data = {
        "data": {"payload": {"item": item}},
        "metadata": {"time": datetime.now(UTC).isoformat()},
        "key": "test-key",
        "__infos__": {
            "failure_timestamp": datetime.now(UTC).isoformat(),
            "retries": 0,
            "reason": "Test failure",
        },
    }

    with failure_file.open("w") as f:
        json.dump(data, f)
        f.write("\n")

    # Run retry with --dry-run
    result = runner.invoke(cli, ["retry", str(failure_file), "--dry-run"])

    assert result.exit_code == 0
    assert "Retried 1 messages" in result.output

    # Verify handles were created in dry-run mode
    handles_dir = tmp_path / "handles"
    assert handles_dir.exists()
    handle_files = list(handles_dir.glob("handles_*.jsonl"))
    assert len(handle_files) > 0
