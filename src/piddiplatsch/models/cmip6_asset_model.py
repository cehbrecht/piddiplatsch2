from typing import Optional, Literal

from pydantic import BaseModel


class CMIP6AssetModel(BaseModel):
    # PID: UUID
    URL: str
    AGGREGATION_LEVEL: Literal["File"]
    FILENAME: str
    CHECKSUM: Optional[str] = None
    SIZE: Optional[int] = None
