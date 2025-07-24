from __future__ import annotations
from typing import Optional, List

from datetime import datetime

from pydantic import BaseModel, Field, model_validator


class HostingNode(BaseModel):
    host: str
    published_on: Optional[datetime] = None


class CMIP6ItemModel(BaseModel):
    URL: str
    AGGREGATION_LEVEL: str = "DATASET"
    DRS_ID: str
    VERSION_NUMBER: Optional[str] = None
    IS_PART_OF: Optional[str] = None
    HAS_PARTS: List[str] = Field(default_factory=list)
    HOSTING_NODE: HostingNode
    REPLICA_NODES: List[HostingNode] = Field(default_factory=list)
    UNPUBLISHED_REPLICAS: List[str] = Field(default_factory=list)
    UNPUBLISHED_HOSTS: List[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_required(self) -> CMIP6ItemModel:
        if not self.HOSTING_NODE or not self.HOSTING_NODE.host:
            raise ValueError("HOSTING_NODE with host is required.")
        return self
