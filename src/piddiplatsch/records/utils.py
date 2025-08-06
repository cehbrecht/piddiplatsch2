import logging
from datetime import datetime

from dateutil.parser import isoparse


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
