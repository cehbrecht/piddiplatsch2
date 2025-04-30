import pytest
from piddiplatsch.cli import cli


def test_send_invalid_path(runner):
    result = runner.invoke(cli, ["send", "--path", "nonexistent.json"])
    assert result.exit_code == 1
    assert "No such file or directory" in result.output
