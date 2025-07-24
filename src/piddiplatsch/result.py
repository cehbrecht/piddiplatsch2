from dataclasses import dataclass
from typing import Optional


@dataclass
class ProcessingResult:
    key: str
    num_handles: int = 0
    success: bool = True
    error: Optional[str] = None
    elapsed: Optional[float] = None
