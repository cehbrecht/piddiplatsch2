from typing import Optional, Literal, List

from pydantic import BaseModel, Field


class CMIP6AssetModel(BaseModel):
    ESGF: str = "ESGF2 TEST"
    URL: str
    AGGREGATION_LEVEL: Literal["FILE"]
    FILE_NAME: str
    IS_PART_OF: str
    CHECKSUM: Optional[str] = None
    CHECKSUM_METHOD: Optional[str] = None
    FILE_SIZE: Optional[int] = None
    FILE_VERSION: Optional[str] = None
    DOWNLOAD_URL: str
    DOWNLOAD_URL_REPLICA: List[str] = Field(default_factory=list)
