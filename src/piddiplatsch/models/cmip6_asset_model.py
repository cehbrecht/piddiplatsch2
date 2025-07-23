from typing import Optional, Literal

# from uuid import UUID

from pydantic import BaseModel


class CMIP6AssetModel(BaseModel):
    # PID: UUID
    PARENT: str
    URL: str
    AGGREGATION_LEVEL: Literal["File"]
    FILENAME: str
    CHECKSUM: Optional[str] = None
    SIZE: Optional[int] = None
