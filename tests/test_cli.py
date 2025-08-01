from piddiplatsch.cli import cli


def test_send_invalid_path(runner):
    result = runner.invoke(cli, ["send", "nonexistent.json"])
    assert result.exit_code == 2
    assert "Usage: cli send [OPTIONS] FILENAME" in result.output
