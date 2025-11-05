import json
from pathlib import Path
from typing import Any

from piddiplatsch.handles.base import HandleBackend


class JsonlHandleBackend(HandleBackend):
    """Store handle records locally as JSONL for testing."""

    def __init__(self, path: str | Path) -> None:
        self.path: Path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def add(self, pid: str, record: dict[str, Any]) -> None:
        """Add or overwrite a PID record (naive append)."""
        with self.path.open("a") as f:
            f.write(json.dumps({"pid": pid, "record": record}) + "\n")

    def get(self, pid: str) -> dict[str, Any] | None:
        """Retrieve a PID record. Return None if not found."""
        if not self.path.exists():
            return None

        with self.path.open() as f:
            for line in f:
                row = json.loads(line)
                if row["pid"] == pid:
                    return row["record"]

        return None

    def update(self, pid: str, record: dict[str, Any]) -> None:
        """Update an existing PID or add if missing."""
        lines: list[dict[str, Any]] = []
        found: bool = False

        if self.path.exists():
            with self.path.open() as f:
                for line in f:
                    row = json.loads(line)
                    if row["pid"] == pid:
                        row["record"] = record
                        found = True
                    lines.append(row)

        if not found:
            lines.append({"pid": pid, "record": record})

        with self.path.open("w") as f:
            for row in lines:
                f.write(json.dumps(row) + "\n")
