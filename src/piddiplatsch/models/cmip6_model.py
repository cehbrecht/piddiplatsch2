from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field, model_validator


class HostingNode(BaseModel):
    host: str
    published_on: datetime | None = None


class BaseCMIP6Model(BaseModel):
    ESGF: str = "ESGF2 TEST"
    URL: str


class CMIP6DatasetModel(BaseCMIP6Model):
    """TODO: clean empty fields
    """

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
        return self


class CMIP6FileModel(BaseCMIP6Model):
    AGGREGATION_LEVEL: str = "FILE"
    FILE_NAME: str
    IS_PART_OF: str
    CHECKSUM: str | None = None
    CHECKSUM_METHOD: str | None = None
    FILE_SIZE: int | None = None
    FILE_VERSION: str | None = None
    DOWNLOAD_URL: str
    DOWNLOAD_URL_REPLICA: list[str] = Field(default_factory=list)
