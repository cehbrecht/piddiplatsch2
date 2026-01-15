"""Fixtures for smoke tests requiring Docker services."""

from pathlib import Path

import pytest

from piddiplatsch.handles.api import get_handle_backend


@pytest.fixture
def testfile(testdata_path: Path):
    """Fixture returning a function to resolve test file paths."""

    def _resolve(*parts) -> Path:
        p = testdata_path.joinpath(*parts)
        if not p.exists():
            raise FileNotFoundError(f"Test file not found: {p}")
        return p

    return _resolve


@pytest.fixture
def handle_client():
    """
    Returns a HandleClient instance connected to the test/mock Handle Service.
    Requires Docker services to be running.
    """
    return get_handle_backend()
