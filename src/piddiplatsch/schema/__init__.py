import json
from functools import cache
from pathlib import Path

SCHEMA_DIR = Path(__file__).parent / "files"  # store all schema files here


@cache
def load_schema(name: str) -> dict:
    """Load and cache a JSON schema by name (e.g. 'cmip6', 'cmip7').

    Args:
        name: The schema name (without .json).

    Returns:
        The loaded JSON schema as a dict.

    """
    schema_file = SCHEMA_DIR / f"{name}.schema.json"
    if not schema_file.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_file}")
    with schema_file.open(encoding="utf-8") as f:
        return json.load(f)


CMIP6_SCHEMA = load_schema("cmip6")
