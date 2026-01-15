"""Shared fixtures for all tests."""

from pathlib import Path

import pytest


@pytest.fixture
def testdata_path() -> Path:
    """Fixture that provides the path to the testdata directory."""
    return Path(__file__).parent / "testdata"
