from __future__ import annotations
from typing import Optional, List

# from uuid import UUID
from datetime import datetime

from pydantic import BaseModel, Field, model_validator


class HostingNode(BaseModel):
    host: str
    published_on: Optional[datetime] = None


class CMIP6ItemModel(BaseModel):
    # PID: UUID
    URL: str
    AGGREGATION_LEVEL: str = "Dataset"
    DATASET_ID: str
    DATASET_VERSION: Optional[str] = None
    HOSTING_NODE: HostingNode
    REPLICA_NODES: List[HostingNode] = Field(default_factory=list)
    UNPUBLISHED_REPLICAS: List[str] = Field(default_factory=list)
    UNPUBLISHED_HOSTS: List[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_required(self) -> CMIP6ItemModel:
        # if not self.PID:
        #    raise ValueError("PID is required.")
        if not self.URL:
            raise ValueError("URL is required.")
        if not self.HOSTING_NODE or not self.HOSTING_NODE.host:
            raise ValueError("HOSTING_NODE with host is required.")
        return self
