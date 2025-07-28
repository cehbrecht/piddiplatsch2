from __future__ import annotations
from typing import Optional, List

from datetime import datetime

from pydantic import BaseModel, Field, model_validator


class HostingNode(BaseModel):
    host: str
    published_on: Optional[datetime] = None


class CMIP6DatasetModel(BaseModel):
    """
    TODO: clean empty fields
    """

    ESGF: str = "ESGF2 TEST"
    URL: str
    AGGREGATION_LEVEL: str = "DATASET"
    DATASET_ID: str
    DATASET_VERSION: Optional[str] = None
    IS_PART_OF: Optional[str] = None
    HAS_PARTS: List[str] = Field(default_factory=list)
    HOSTING_NODE: HostingNode
    REPLICA_NODES: List[HostingNode] = Field(default_factory=list)  # one per replica
    UNPUBLISHED_REPLICAS: List[str] = Field(default_factory=list)  # same as replica
    UNPUBLISHED_HOSTS: Optional[HostingNode] = None

    @model_validator(mode="after")
    def validate_required(self) -> CMIP6DatasetModel:
        if not self.HOSTING_NODE or not self.HOSTING_NODE.host:
            raise ValueError("HOSTING_NODE with host is required.")
        return self


class CMIP6FileModel(BaseModel):
    ESGF: str = "ESGF2 TEST"
    URL: str
    AGGREGATION_LEVEL: str = "FILE"
    FILE_NAME: str
    IS_PART_OF: str
    CHECKSUM: Optional[str] = None
    CHECKSUM_METHOD: Optional[str] = None
    FILE_SIZE: Optional[int] = None
    FILE_VERSION: Optional[str] = None
    DOWNLOAD_URL: str
    DOWNLOAD_URL_REPLICA: List[str] = Field(default_factory=list)
