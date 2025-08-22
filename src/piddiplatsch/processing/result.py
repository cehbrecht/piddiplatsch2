from dataclasses import dataclass


@dataclass
class ProcessingResult:
    key: str
    num_handles: int = 0
    success: bool = True
    error: str | None = None
    skipped: bool = False
    elapsed: float | None = None

    # Detailed timings
    handle_processing_time: float | None = None
