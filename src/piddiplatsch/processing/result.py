from dataclasses import dataclass


@dataclass
class ProcessingResult:
    key: str
    num_handles: int = 0
    success: bool = True
    error: str | None = None
    elapsed: float | None = None
