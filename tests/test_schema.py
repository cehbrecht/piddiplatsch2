import pytest

from piddiplatsch.schema import load_schema


def test_load_cmip6_schema():
    """Test loading the CMIP6 JSON schema."""
    schema = load_schema("cmip6")

    assert isinstance(schema, dict), "Schema should be loaded as a dictionary"
    assert schema.get("type") == "object"
    assert "properties" in schema
    assert "id" in schema["properties"]
    assert "assets" in schema["properties"]


def test_load_nonexistent_schema_raises():
    with pytest.raises(FileNotFoundError):
        load_schema("not_a_real_schema")
