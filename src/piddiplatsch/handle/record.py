import uuid
import warnings
from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field, model_validator


class HostingNode(BaseModel):
    host: str
    published_on: Optional[datetime] = None


class CMIP6HandleModel(BaseModel):
    PID: uuid.UUID
    URL: str
    AGGREGATION_LEVEL: str = "Dataset"
    DATASET_ID: str
    DATASET_VERSION: Optional[str] = None
    HOSTING_NODE: HostingNode
    REPLICA_NODE: list[str] = Field(default_factory=list)
    UNPUBLISHED_REPLICAS: list[str] = Field(default_factory=list)
    UNPUBLISHED_HOSTS: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def check_required_fields(self) -> "CMIP6HandleModel":
        if not self.PID:
            raise ValueError("PID is required")
        if not self.URL:
            raise ValueError("URL is required")
        if not self.HOSTING_NODE or not self.HOSTING_NODE.host:
            raise ValueError("HOSTING_NODE with host is required")
        return self

    def to_handle_record(self) -> list[dict[str, Any]]:
        """Convert the model to a Handle-compatible JSON record."""
        record = []
        index = 1

        def add_field(field_type: str, value: Any):
            nonlocal index
            record.append({
                "index": index,
                "type": field_type,
                "data": {"format": "string", "value": str(value)}
            })
            index += 1

        add_field("PID", self.PID)
        add_field("URL", self.URL)
        add_field("AGGREGATION_LEVEL", self.AGGREGATION_LEVEL)
        add_field("DATASET_ID", self.DATASET_ID)
        if self.DATASET_VERSION:
            add_field("DATASET_VERSION", self.DATASET_VERSION)
        if self.HOSTING_NODE.host:
            add_field("HOSTING_NODE", self.HOSTING_NODE.host)
        if self.HOSTING_NODE.published_on:
            add_field("PUBLISHED_ON", self.HOSTING_NODE.published_on.isoformat())

        for host in self.REPLICA_NODE:
            add_field("REPLICA_NODE", host)
        for host in self.UNPUBLISHED_REPLICAS:
            add_field("UNPUBLISHED_REPLICAS", host)
        for host in self.UNPUBLISHED_HOSTS:
            add_field("UNPUBLISHED_HOSTS", host)

        return record


class CMIP6HandleBuilder:
    """Builds a validated CMIP6HandleModel from a STAC item."""

    def __init__(self, item: dict[str, Any]):
        self.item = item

    def build(self) -> CMIP6HandleModel:
        id_str = self.item.get("id")
        if not id_str:
            raise ValueError("Item 'id' is missing")

        pid = uuid.uuid3(uuid.NAMESPACE_URL, id_str)
        url = self._extract_url(self.item)
        dataset_id, version = self._split_id(id_str)
        hosting_node = self._extract_hosting_node(self.item)

        return CMIP6HandleModel(
            PID=pid,
            URL=url,
            DATASET_ID=dataset_id,
            DATASET_VERSION=version,
            HOSTING_NODE=hosting_node,
        )

    @staticmethod
    def _split_id(full_id: str) -> tuple[str, Optional[str]]:
        parts = full_id.rsplit(".", 1)
        if len(parts) == 2:
            return parts[0], parts[1]
        return full_id, None

    @staticmethod
    def _extract_url(item: dict[str, Any]) -> str:
        try:
            return item["links"][0]["href"]
        except (KeyError, IndexError) as e:
            raise ValueError("Missing 'links[0].href' in item") from e

    @staticmethod
    def _extract_hosting_node(item: dict[str, Any]) -> HostingNode:
        locations = item.get("locations", [])
        for loc in locations:
            if "host" in loc:
                try:
                    return HostingNode(
                        host=loc["host"],
                        published_on=loc.get("publishedOn"),
                    )
                except Exception as e:
                    warnings.warn(f"Invalid hosting node metadata: {e}", stacklevel=2)

        # Fallback logic
        assets = item.get("assets", {})
        fallback_host = (
            assets.get("reference_file", {}).get("alternate:name")
            or assets.get("data0001", {}).get("alternate:name")
            or "unknown"
        )
        warnings.warn("Using fallback for HOSTING_NODE", stacklevel=2)
        return HostingNode(host=fallback_host)
