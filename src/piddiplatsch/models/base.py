from __future__ import annotations

import logging
from datetime import datetime

from pydantic import BaseModel

from piddiplatsch.config import config

logger = logging.getLogger(__name__)


ALLOWED_CHECKSUM_METHODs = {
    "md5",
    "sha1",
    "sha2-256",
    "sha2-512",
    "sha3-256",
    "sha3-512",
    "blake2b-256",
}


def get_max_parts() -> int:
    """Read max_parts dynamically from config."""
    return config.get("cmip6", {}).get("max_parts", -1)


def strict_mode() -> bool:
    """Check if strict schema validation is enabled via config."""
    return config.get("schema", {}).get("strict_mode", False)


class HostingNode(BaseModel):
    host: str
    published_on: datetime | None = None


"""
CMIP6-specific base model moved to piddiplatsch.plugins.cmip6.base.
This module retains shared utilities used across models.
"""
