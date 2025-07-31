from uuid import NAMESPACE_URL, uuid3


def item_pid(item_id: str) -> str:
    """Return a deterministic UUIDv3 PID for the given STAC item ID."""
    return str(uuid3(NAMESPACE_URL, item_id))


def asset_pid(item_id: str, asset_key: str) -> str:
    """Return a deterministic UUIDv3 PID for a given asset of a STAC item."""
    return str(uuid3(NAMESPACE_URL, f"{item_id}#{asset_key}"))
