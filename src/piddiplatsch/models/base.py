from __future__ import annotations

import logging
import uuid
from datetime import datetime

from pydantic import (
    BaseModel,
    HttpUrl,
    PrivateAttr,
    field_serializer,
)

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


class BaseCMIP6Model(BaseModel):
    _PID: str | None = PrivateAttr(default=None)

    ESGF: str = "ESGF2 TEST"
    URL: HttpUrl

    # --- Ensure URLs serialize as strings ---
    @field_serializer("URL")
    def _serialize_url(self, v: HttpUrl) -> str:
        return str(v)

    def set_pid(self, value: str | uuid.UUID) -> None:
        """Set and validate the PID (must be a valid UUID or UUID string)."""
        if isinstance(value, uuid.UUID):
            self._PID = str(value)
        elif isinstance(value, str):
            try:
                self._PID = str(uuid.UUID(value))
            except ValueError:
                raise ValueError(f"Invalid PID string: {value} is not a valid UUID.")
        else:
            raise TypeError(
                f"PID must be a UUID or UUID string, got {type(value).__name__}"
            )

    def get_pid(self) -> str | None:
        return self._PID
