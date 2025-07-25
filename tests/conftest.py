import pytest
from pathlib import Path
from click.testing import CliRunner


@pytest.fixture
def testdata_path() -> Path:
    """Fixture that provides the path to the testdata directory."""
    return Path(__file__).parent / "testdata"


@pytest.fixture
def testfile(testdata_path: Path):
    """Fixture returning a function to resolve test file paths."""

    def _resolve(*parts) -> Path:
        p = testdata_path.joinpath(*parts)
        if not p.exists():
            raise FileNotFoundError(f"Test file not found: {p}")
        return p

    return _resolve


def pytest_runtest_setup(item):
    if "online" in item.keywords and not item.config.getoption("-m"):
        pytest.skip("Skipping online test since '-m online' was not specified.")


@pytest.fixture
def runner():
    return CliRunner()
