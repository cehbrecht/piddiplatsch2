import json
from pathlib import Path
from typing import Any

from piddiplatsch.config import config
from piddiplatsch.handles.base import HandleBackend


class JsonlHandleBackend(HandleBackend):
    """Store handle records locally as JSONL for testing."""

    def __init__(self) -> None:
        self.path = (
            Path(config.get("consumer", {}).get("output_dir", "outputs"))
            / "handles"
            / "records.jsonl"
        )
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def _store(self, handle: str, handle_data: dict[str, Any]) -> None:
        location = handle_data.pop("URL", None)
        entry = {"handle": handle, "URL": location, "data": handle_data}
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
