import pytest
import os


@pytest.fixture
def testdata_path():
    """Fixture that provides the path to the testdata directory."""
    return os.path.join(os.path.dirname(__file__), "testdata")


def pytest_runtest_setup(item):
    if "online" in item.keywords and not item.config.getoption("-m"):
        pytest.skip("Skipping online test since '-m online' was not specified.")
