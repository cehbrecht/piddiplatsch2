import json
from abc import ABC, abstractmethod
from collections.abc import Iterable
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


class JsonlRecorder:
    """High-level recorder for appending JSON objects to daily JSONL files.

    Example:
        recorder = JsonlRecorder(Path("outputs")/"failures", "failed_items")
        recorder.record(data, infos={"reason": "..."})
    """

    def __init__(self, root_dir: Path, prefix: str):
        self.root_dir = Path(root_dir)
        self.prefix = prefix
        self.writer = DailyJsonlWriter(self.root_dir)

    def record(
        self, data: dict, infos: dict | None = None, subdir: Path | None = None
    ) -> Path:
        payload = DailyJsonlWriter.wrap_with_infos(data, infos) if infos else data
        return self.writer.write(self.prefix, payload, subdir=subdir)


class RecorderBase(ABC):
    """Abstract base for high-level category recorders.

    Subclasses provide how to prepare the payload/infos/subdir while this class
    implements the common write orchestration to a daily JSONL using
    `DailyJsonlWriter`.

    Implement `prepare()` to shape the record and optional subdirectory.
    """

    def __init__(self, root_dir: Path, prefix: str):
        self.root_dir = Path(root_dir)
        self.prefix = prefix
        self.writer = DailyJsonlWriter(self.root_dir)

    @abstractmethod
    def prepare(
        self,
        key: str,
        data: dict,
        reason: str | None,
        retries: int | None,
    ) -> tuple[dict, dict | None, Path | None]:
        """Return a tuple of (payload, infos, subdir).

        - `payload`: dict to be written (before infos wrapping)
        - `infos`: optional metadata dict to be wrapped under `__infos__`
        - `subdir`: optional absolute directory where the daily file should be written
        """

    def write(
        self,
        key: str,
        data: dict,
        *,
        reason: str | None = None,
        retries: int | None = None,
    ) -> Path:
        payload, infos, subdir = self.prepare(key, data, reason, retries)
        if infos:
            payload = DailyJsonlWriter.wrap_with_infos(payload, infos)
        return self.writer.write(self.prefix, payload, subdir=subdir)


def read_jsonl(file_path: Path) -> list[dict]:
    """Read a JSONL file and return list of dicts. Returns empty list if missing."""
    if not file_path.exists():
        return []
    records: list[dict] = []
    with file_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except Exception:
                # Skip malformed lines but keep going
                continue
    return records


def find_jsonl(paths: Iterable[Path]) -> list[Path]:
    """Resolve a sequence of files/dirs/globs to a sorted unique list of JSONL file paths."""
    files: set[Path] = set()
    for path in paths:
        if path.is_file():
            if path.suffix == ".jsonl":
                files.add(path)
        elif path.is_dir():
            files.update(p for p in path.glob("*.jsonl") if p.is_file())
        else:
            parent = path.parent
            pattern = path.name
            files.update(
                p for p in parent.glob(pattern) if p.is_file() and p.suffix == ".jsonl"
            )
    return sorted(files)
