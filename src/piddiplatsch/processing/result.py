from dataclasses import dataclass


@dataclass
class ProcessingResult:
    key: str
    num_handles: int = 0
    success: bool = False
    error: str | None = None
    skipped: bool = False
    patched: bool = False
    elapsed: float = 0.0

    # Detailed timings
    handle_processing_time: float = 0.0
