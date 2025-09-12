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


def get_max_parts() -> int:
    """Read max_parts dynamically from config."""
    return config.get("cmip6", {}).get("max_parts", -1)


def strict_mode() -> bool:
    """Check if strict schema validation is enabled via config."""
    return config.get("schema", {}).get("strict", False)


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
    CHECKSUM_METHOD: str = "AUTO"
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

        checksum = self.CHECKSUM.lower()
        method = self.CHECKSUM_METHOD.upper()

        # AUTO-detect algorithm if requested
        if method == "AUTO":
            method = detect_checksum_type(checksum)
            if method == "UNKNOWN":
                if strict_mode():
                    raise ValueError(
                        f"Could not auto-detect checksum type for value "
                        f"{checksum!r} (length={len(checksum)})"
                    )
                else:
                    logger.warning(
                        "Accepted checksum %r with UNKNOWN method (lenient mode)",
                        checksum,
                    )
            self.CHECKSUM_METHOD = method

        # Validate according to chosen/detected method
        if method == "SHA1":
            if not re.fullmatch(r"[0-9a-f]{40}", checksum):
                raise ValueError("Invalid SHA-1 checksum (must be 40 hex chars).")

        elif method == "SHA256":
            if not re.fullmatch(r"[0-9a-f]{64}", checksum):
                raise ValueError("Invalid SHA-256 checksum (must be 64 hex chars).")

        elif method == "SHA512":
            if not re.fullmatch(r"[0-9a-f]{128}", checksum):
                raise ValueError("Invalid SHA-512 checksum (must be 128 hex chars).")

        elif method == "SHA256-MULTIHASH":
            if not (
                checksum.startswith("1220") and re.fullmatch(r"[0-9a-f]{68}", checksum)
            ):
                raise ValueError(
                    "Invalid multihash SHA-256 (must start with '1220' + 64 hex chars)."
                )

        elif method == "UNKNOWN":
            if strict_mode():
                raise ValueError(
                    f"Checksum {self.CHECKSUM!r} could not be validated under strict mode"
                )
            # lenient mode → already warned, so accept

        else:
            raise ValueError(f"Unsupported CHECKSUM_METHOD: {method}")

        return self
