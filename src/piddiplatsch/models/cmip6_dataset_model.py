from piddiplatsch.models.base import BaseCMIP6Model, strict_mode, get_max_parts, HostingNode

from pydantic import (
    PrivateAttr,
    Field,
    model_validator,
)

from datetime import datetime


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
        if max_parts > 0 and len(self.HAS_PARTS) > max_parts:
            raise ValueError(
                f"Too many parts: {len(self.HAS_PARTS)} exceeds max_parts={max_parts}"
            )
        return self
