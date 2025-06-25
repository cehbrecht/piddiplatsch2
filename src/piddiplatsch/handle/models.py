from __future__ import annotations
from typing import Optional
from uuid import UUID
from datetime import datetime

from pydantic import BaseModel, Field, model_validator


class HostingNode(BaseModel):
    host: str
    published_on: Optional[datetime] = None


class CMIP6HandleModel(BaseModel):
    PID: UUID
    URL: str
    AGGREGATION_LEVEL: str = "Dataset"
    DATASET_ID: str
    DATASET_VERSION: Optional[str] = None
    HOSTING_NODE: HostingNode
    REPLICA_NODE: list[str] = Field(default_factory=list)
    UNPUBLISHED_REPLICAS: list[str] = Field(default_factory=list)
    UNPUBLISHED_HOSTS: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_required(self) -> CMIP6HandleModel:
        if not self.PID:
            raise ValueError("PID is required.")
        if not self.URL:
            raise ValueError("URL is required.")
        if not self.HOSTING_NODE or not self.HOSTING_NODE.host:
            raise ValueError("HOSTING_NODE with host is required.")
        return self

    def model_dump_handle(self) -> list[dict]:
        """Returns a pyhandle-compatible list of typed record entries."""
        index = 1
        output: list[dict] = []

        def add_entry(type_str: str, value: str):
            nonlocal index
            output.append({
                "index": index,
                "type": type_str,
                "data": {
                    "format": "string",
                    "value": value
                }
            })
            index += 1

        add_entry("PID", str(self.PID))
        add_entry("URL", self.URL)
        add_entry("AGGREGATION_LEVEL", self.AGGREGATION_LEVEL)
        add_entry("DATASET_ID", self.DATASET_ID)

        if self.DATASET_VERSION:
            add_entry("DATASET_VERSION", self.DATASET_VERSION)

        if self.HOSTING_NODE.host:
            add_entry("HOSTING_NODE", self.HOSTING_NODE.host)

        if self.HOSTING_NODE.published_on:
            add_entry("PUBLISHED_ON", self.HOSTING_NODE.published_on.isoformat())

        for replica in self.REPLICA_NODE:
            add_entry("REPLICA_NODE", replica)

        for replica in self.UNPUBLISHED_REPLICAS:
            add_entry("UNPUBLISHED_REPLICAS", replica)

        for host in self.UNPUBLISHED_HOSTS:
            add_entry("UNPUBLISHED_HOSTS", host)

        return output
