from __future__ import annotations

from pydantic import Field, HttpUrl, PositiveInt, field_serializer, model_validator

from piddiplatsch.models.base import (
    ALLOWED_CHECKSUM_METHODs,
    BaseCMIP6Model,
    strict_mode,
)


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
        return self
