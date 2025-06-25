from __future__ import annotations
from typing import Optional, List
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
    REPLICA_NODES: List[HostingNode] = Field(default_factory=list)
    UNPUBLISHED_REPLICAS: List[str] = Field(default_factory=list)
    UNPUBLISHED_HOSTS: List[str] = Field(default_factory=list)

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
        """Return pyhandle-compatible list of typed record entries with repeated types."""

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

        # Required fields
        add_entry("PID", str(self.PID))
        add_entry("URL", self.URL)
        add_entry("AGGREGATION_LEVEL", self.AGGREGATION_LEVEL)
        add_entry("DATASET_ID", self.DATASET_ID)

        # Optional version
        if self.DATASET_VERSION:
            add_entry("DATASET_VERSION", self.DATASET_VERSION)

        # Hosting node with optional published_on
        if self.HOSTING_NODE.host:
            host_val = self.HOSTING_NODE.host
            if self.HOSTING_NODE.published_on:
                host_val += "|" + self.HOSTING_NODE.published_on.isoformat()
            add_entry("HOSTING_NODE", host_val)

        # Replica nodes, one entry per node, each with optional published_on
        for replica in self.REPLICA_NODES:
            rep_val = replica.host
            if replica.published_on:
                rep_val += "|" + replica.published_on.isoformat()
            add_entry("REPLICA_NODE", rep_val)

        # Unpublished replicas (strings)
        for rep in self.UNPUBLISHED_REPLICAS:
            add_entry("UNPUBLISHED_REPLICAS", rep)

        # Unpublished hosts (strings)
        for host in self.UNPUBLISHED_HOSTS:
            add_entry("UNPUBLISHED_HOSTS", host)

        return output

    def as_record(self) -> dict:
        """Return model as dict for inspection or testing."""
        return self.model_dump()

    def as_json(self) -> str:
        """Return model serialized as JSON string."""
        return self.model_dump_json()
