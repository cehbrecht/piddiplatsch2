import pytest

from piddiplatsch.config.schema import validate_config


def _base_config():
    return {
        "consumer": {"processor": "cmip6", "topic": "CMIP6"},
        # Keep handle as jsonl for tests to avoid pyhandle requirements
        "handle": {"backend": "jsonl"},
        # Disable lookups unless explicitly tested
        "lookup": {"enabled": False},
    }


def test_invalid_bootstrap_servers_format():
    cfg = _base_config()
    cfg["kafka"] = {"bootstrap.servers": "localhost"}  # missing :port
    errors, warnings = validate_config(cfg)
    assert errors, "Expected errors for invalid bootstrap.servers format"
    assert any(
        "bootstrap.servers" in e and "host:port" in e for e in errors
    ), f"Unexpected errors: {errors}"


def test_valid_bootstrap_servers_format():
    cfg = _base_config()
    cfg["kafka"] = {"bootstrap.servers": "localhost:39092"}
    errors, warnings = validate_config(cfg)
    assert not errors, f"Did not expect errors: {errors}"


def test_lookup_stac_requires_base_url():
    cfg = _base_config()
    cfg["kafka"] = {"bootstrap.servers": "localhost:39092"}
    cfg["lookup"] = {"enabled": True, "backend": "stac"}
    # omit [stac].base_url
    errors, warnings = validate_config(cfg)
    assert errors, "Expected error for missing [stac].base_url"
    assert any(
        "[stac].base_url" in e for e in errors
    ), f"Missing base_url error not found in: {errors}"


def test_lookup_es_requires_base_url():
    cfg = _base_config()
    cfg["kafka"] = {"bootstrap.servers": "localhost:39092"}
    cfg["lookup"] = {"enabled": True, "backend": "es"}
    # omit [elasticsearch].base_url
    errors, warnings = validate_config(cfg)
    assert errors, "Expected error for missing [elasticsearch].base_url"
    assert any(
        "[elasticsearch].base_url" in e for e in errors
    ), f"Missing base_url error not found in: {errors}"
