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
    schema_validation_time: float | None = None
    record_validation_time: float | None = None
    handle_processing_time: float | None = None
