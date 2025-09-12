import logging
from datetime import datetime
from uuid import NAMESPACE_URL, uuid3

from dateutil.parser import isoparse

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


def detect_checksum_type(checksum: str) -> str:
    """Guess checksum algorithm from hex length or multihash prefix."""
    length = len(checksum)

    if checksum.startswith("1220") and length == 68:
        return "SHA256-MULTIHASH"
    if length == 32:
        return "MD5"
    if length == 40:
        return "SHA1"
    if length == 64:
        return "SHA256"
    if length == 128:
        return "SHA512"

    # Unknown checksum type
    return "UNKNOWN"
