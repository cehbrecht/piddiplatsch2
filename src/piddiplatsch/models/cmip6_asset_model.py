from typing import Optional, Literal

from pydantic import BaseModel


class CMIP6AssetModel(BaseModel):
    URL: str
    AGGREGATION_LEVEL: Literal["FILE"]
    FILE_NAME: str
    IS_PART_OF: str
    CHECKSUM: Optional[str] = None
    FILE_SIZE: Optional[int] = None
    FILE_VERSION: Optional[str] = None
    CHECKSUM_METHOD: Optional[str] = None
