import json
from datetime import UTC, datetime
from pathlib import Path


class DailyJsonlWriter:
    """Utility for writing JSONL records to daily-rotated files.

    - Uses a directory (root) and a filename prefix (e.g., 'skipped_items').
    - Appends one JSON object per line.
    - Returns the path of the written file.
    """

    def __init__(self, root_dir: Path):
        self.root_dir = Path(root_dir)
        self.root_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def wrap_with_infos(data: dict, infos: dict) -> dict:
        # Merge metadata under `__infos__` key
        wrapped = {**data, "__infos__": infos}
        return wrapped

    def write(self, prefix: str, data: dict, subdir: Path | None = None) -> Path:
        now = datetime.now(UTC)
        dated_filename = f"{prefix}_{now.date()}.jsonl"
        target_dir = Path(subdir) if subdir else self.root_dir
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / dated_filename
        with target_path.open("a", encoding="utf-8") as f:
            json.dump(data, f)
            f.write("\n")
        return target_path
