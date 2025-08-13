from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel, Field, PrivateAttr, model_validator


class HostingNode(BaseModel):
    host: str
    published_on: datetime | None = None


class BaseCMIP6Model(BaseModel):
    _pid: str | None = PrivateAttr(default=None)

    ESGF: str = "ESGF2 TEST"
    URL: str

    def set_pid(self, value: str | uuid.UUID) -> None:
        """Set and validate the PID (must be a valid UUID or UUID string)."""
        if isinstance(value, uuid.UUID):
            self._pid = str(value)
        elif isinstance(value, str):
            try:
                self._pid = str(uuid.UUID(value))
            except ValueError:
                raise ValueError(f"Invalid PID string: {value} is not a valid UUID.")
        else:
            raise TypeError(
                f"PID must be a UUID or UUID string, got {type(value).__name__}"
            )

    def get_pid(self) -> str | None:
        return self._pid


class CMIP6DatasetModel(BaseCMIP6Model):
    AGGREGATION_LEVEL: str = "DATASET"
    DATASET_ID: str
    DATASET_VERSION: str | None = None
    IS_PART_OF: str | None = None
    HAS_PARTS: list[str] = Field(default_factory=list)
    HOSTING_NODE: HostingNode
    REPLICA_NODES: list[HostingNode] = Field(default_factory=list)  # one per replica
    UNPUBLISHED_REPLICAS: list[str] = Field(default_factory=list)  # same as replica
    UNPUBLISHED_HOSTS: HostingNode | None = None

    @model_validator(mode="after")
    def validate_required(self) -> CMIP6DatasetModel:
        if not self.HOSTING_NODE or not self.HOSTING_NODE.host:
            raise ValueError("HOSTING_NODE with host is required.")
        if not self.HAS_PARTS:
            raise ValueError("HAS_PARTS must contain at least one file.")
        return self


class CMIP6FileModel(BaseCMIP6Model):
    AGGREGATION_LEVEL: str = "FILE"
    FILE_NAME: str
    IS_PART_OF: str
    CHECKSUM: str
    CHECKSUM_METHOD: str = "SHA256"
    FILE_SIZE: int
    FILE_VERSION: str | None = None
    DOWNLOAD_URL: str
    DOWNLOAD_URL_REPLICA: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_required(self) -> CMIP6FileModel:
        if not self.FILE_SIZE:
            raise ValueError("FILE_SIZE is required.")
        if not self.CHECKSUM:
            raise ValueError("CHECKSUM is required.")
        return self
