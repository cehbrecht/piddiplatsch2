"""Fixtures for smoke tests requiring Docker services."""

import subprocess  # noqa: S404
import time
from pathlib import Path

import pytest

from piddiplatsch.handles.api import get_handle_backend


@pytest.fixture(scope="session", autouse=True)
def docker_services():
    """
    Start Docker services (Kafka + Handle server) before smoke tests and stop after.
    Uses Makefile targets: 'make start' and 'make stop'.
    """
    print("\n" + "=" * 70)
    print("🐳 Starting Docker services (Kafka + Handle server)...")
    print("=" * 70)
    subprocess.run(["make", "start"], check=True)  # noqa: S603, S607

    # Give services time to initialize
    print("⏳ Waiting 5 seconds for services to initialize...")
    time.sleep(5)
    print("✅ Docker services ready!\n")

    yield

    print("\n" + "=" * 70)
    print("🐳 Stopping Docker services...")
    print("=" * 70)
    subprocess.run(["make", "stop"], check=True)  # noqa: S603, S607
    print("✅ Docker services stopped!\n")


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
