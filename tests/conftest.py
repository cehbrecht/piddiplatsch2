"""Shared fixtures for all tests."""

import pytest


def pytest_runtest_setup(item):
    """Skip online tests unless explicitly requested (for backward compatibility)."""
    if "online" in item.keywords and not item.config.getoption("-m"):
        pytest.skip("Skipping online test since '-m online' was not specified.")
