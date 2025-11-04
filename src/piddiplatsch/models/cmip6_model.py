from __future__ import annotations

import logging
import re
import uuid
from datetime import datetime

from pydantic import (
    BaseModel,
    Field,
    HttpUrl,
    PositiveInt,
    PrivateAttr,
    field_serializer,
    model_validator,
)

from piddiplatsch.config import config
from piddiplatsch.utils.models import detect_checksum_type

logger = logging.getLogger(__name__)

ALLOWED_CHECKSUM_TYPES = {
    "sha2-256",
    "sha2-512",
    "sha3-256",
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


class CMIP6DatasetModel(BaseCMIP6Model):
    AGGREGATION_LEVEL: str = "DATASET"
    DATASET_ID: str
    DATASET_VERSION: str | None = None
    PREVIOUS_VERSION: str | None = None
    IS_PART_OF: str | None = None
    HAS_PARTS: list[str] = Field(default_factory=list)
    HOSTING_NODE: HostingNode
    REPLICA_NODES: list[HostingNode] = Field(default_factory=list)
    _RETRACTED: bool | None = PrivateAttr(default=False)
    RETRACTED_ON: datetime | None = None

    @model_validator(mode="after")
    def validate_required(self) -> CMIP6DatasetModel:
        if not self.HOSTING_NODE:
            raise ValueError("HOSTING_NODE is required.")

        max_parts = get_max_parts()
        if max_parts != 0 and not self.HAS_PARTS:
            if strict_mode():
                raise ValueError("HAS_PARTS must contain at least one file.")
        if max_parts > 0 and len(self.HAS_PARTS) > max_parts:
            raise ValueError(
                f"Too many parts: {len(self.HAS_PARTS)} exceeds max_parts={max_parts}"
            )
        return self


class CMIP6FileModel(BaseCMIP6Model):
    AGGREGATION_LEVEL: str = "FILE"
    FILE_NAME: str
    IS_PART_OF: str
    CHECKSUM: str
    CHECKSUM_METHOD: str
    FILE_SIZE: PositiveInt
    DOWNLOAD_URL: HttpUrl
    REPLICA_DOWNLOAD_URLS: list[HttpUrl] = Field(default_factory=list)

    # --- Ensure URLs serialize as strings ---
    @field_serializer("DOWNLOAD_URL", "REPLICA_DOWNLOAD_URLS")
    def _serialize_urls(self, v) -> str | list[str]:
        if isinstance(v, list):
            return [str(u) for u in v]
        return str(v)

    @model_validator(mode="after")
    def validate_required(self) -> CMIP6FileModel:
        if not self.CHECKSUM:
            raise ValueError("CHECKSUM is required.")
        if not self.CHECKSUM_METHOD:
            raise ValueError("CHECKSUM_METHOD is required.")
        if self.CHECKSUM_METHOD not in ALLOWED_CHECKSUM_TYPES:
            if strict_mode():
                raise ValueError(f"Used CHECKSUM_METHOD is not allowed: {self.CHECKSUM_METHOD}")
        return self
