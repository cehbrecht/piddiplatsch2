import logging
from datetime import datetime
from uuid import NAMESPACE_URL, uuid3

from dateutil.parser import isoparse
from multiformats import multihash

from piddiplatsch.config import config


def build_handle(pid: str, as_uri: bool = False) -> str:
    scheme = "hdl"
    prefix = config.get("handle", {}).get("prefix", "")
    if as_uri:
        handle = f"{scheme}:{prefix}/{pid}"
    else:
        handle = f"{prefix}/{pid}"
    return handle


def item_pid(item_id: str) -> str:
    """Return a deterministic UUIDv3 PID for the given STAC item ID."""
    return str(uuid3(NAMESPACE_URL, item_id))


def asset_pid(item_id: str, asset_key: str) -> str:
    """Return a deterministic UUIDv3 PID for a given asset of a STAC item."""
    return str(uuid3(NAMESPACE_URL, f"{item_id}#{asset_key}"))


def drop_empty(d):
    """Recursively clean empty fields"""
    result = {}
    for k, v in d.items():
        if v in ("", [], {}, None):
            continue
        if isinstance(v, dict):
            nested = drop_empty(v)
            if nested:
                result[k] = nested
        else:
            result[k] = v
    return result


def parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return isoparse(value)
    except Exception:
        logging.warning(f"Failed to parse datetime: {value}")
        return None


def parse_pid(value: str) -> str:
    if value and isinstance(value, str) and "/" in value:
        pid_ = value.split("/")[1]
    else:
        pid_ = value
    return pid_


def parse_multihash_hex(checksum_with_type: str) -> tuple[str, str]:
    """
    Parse a multihash hex string into (checksum_type, checksum_hex).

    Args:
        checksum_with_type: multihash as hex string, e.g.
            '12205994471abb01112afcc18159f6cc74b4f511b99806da59b3caf5a9c173cacfc5'

    Returns:
        Tuple of:
        - checksum_type: human-readable hash name, e.g., 'sha2-256'
        - checksum_hex: raw digest as hex string
    """
    # Convert hex string to bytes
    mh_bytes = bytes.fromhex(checksum_with_type)

    # Decode the multihash
    code, digest_bytes = multihash.unwrap_raw(mh_bytes)

    # Get human-readable hash name
    checksum_type = multihash.get(code).name

    # Convert digest bytes to hex
    checksum_hex = digest_bytes.hex()

    return checksum_type, checksum_hex