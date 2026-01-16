"""Shared fixtures for all tests."""

from pathlib import Path

import pytest

from piddiplatsch.config import config as _config


@pytest.fixture(scope="session", autouse=True)
def _load_tests_config():
    """Load shared test config for all suites (unit, integration, smoke)."""
    cfg_path = Path(__file__).parent / "config.toml"
    _config.load_user_config(str(cfg_path))


@pytest.fixture
def testdata_path() -> Path:
    """Fixture that provides the path to the testdata directory."""
    return Path(__file__).parent / "testdata"
