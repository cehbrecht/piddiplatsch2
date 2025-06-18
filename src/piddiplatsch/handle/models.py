from pydantic import BaseModel, HttpUrl, Field, ConfigDict, field_validator
from typing import Optional, List
import uuid


class CMIP6HandleRecordModel(BaseModel):
    model_config = ConfigDict(
        strict=True,
        extra="forbid",
        json_encoders={uuid.UUID: lambda v: str(v)},
        populate_by_name=True,
    )

    PID: uuid.UUID = Field(..., alias="pid")
    URL: HttpUrl = Field(..., alias="url")
    AGGREGATION_LEVEL: str = Field(default="Dataset", alias="aggregation_level")
    DATASET_ID: str = Field(..., alias="dataset_id")
    DATASET_VERSION: Optional[str] = Field(default=None, alias="dataset_version")
    HOSTING_NODE: str = Field(..., alias="hosting_node")
    REPLICA_NODE: List[str] = Field(default_factory=list, alias="replica_node")
    UNPUBLISHED_REPLICAS: List[str] = Field(
        default_factory=list, alias="unpublished_replicas"
    )
    UNPUBLISHED_HOSTS: List[str] = Field(
        default_factory=list, alias="unpublished_hosts"
    )

    @field_validator("AGGREGATION_LEVEL")
    @classmethod
    def check_dataset(cls, v: str) -> str:
        if v != "Dataset":
            raise ValueError("AGGREGATION_LEVEL must be 'Dataset'")
        return v
