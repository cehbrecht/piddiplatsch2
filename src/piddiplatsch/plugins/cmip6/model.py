from __future__ import annotations

from datetime import datetime

from pydantic import (
    Field,
    HttpUrl,
    PositiveInt,
    PrivateAttr,
    field_serializer,
    model_validator,
)

from piddiplatsch.models.base import (
    ALLOWED_CHECKSUM_METHODs,
    BaseCMIP6Model,
    HostingNode,
    get_max_parts,
    strict_mode,
)
from piddiplatsch.monitoring import stats


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
            else:
                stats.warn(message="HAS_PARTS must contain at least one file.")
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
    def validate_checksum(self) -> CMIP6FileModel:
        if not self.CHECKSUM:
            raise ValueError("CHECKSUM is required.")
        if not self.CHECKSUM_METHOD:
            raise ValueError("CHECKSUM_METHOD is required.")
        if self.CHECKSUM_METHOD not in ALLOWED_CHECKSUM_METHODs:
            if strict_mode():
                raise ValueError(
                    f"Used CHECKSUM_METHOD is not allowed: {self.CHECKSUM_METHOD}"
                )
            else:
                stats.warn(
                    message=f"Used CHECKSUM_METHOD is not allowed: {self.CHECKSUM_METHOD}"
                )
        return self


__all__ = [
    "CMIP6DatasetModel",
    "CMIP6FileModel",
]
