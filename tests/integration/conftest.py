"""Fixtures for integration tests using JSONL backend."""

import pytest
from click.testing import CliRunner


@pytest.fixture
def runner():
    """Provides a Click CLI test runner."""
    return CliRunner()
