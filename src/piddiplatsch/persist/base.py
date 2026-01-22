import logging
from abc import ABC, abstractmethod
from pathlib import Path

from piddiplatsch.persist.helpers import DailyJsonlWriter
from piddiplatsch.result import PrepareResult


class RecorderBase(ABC):
    LOG_KIND = "record"
    LOG_LEVEL = logging.INFO
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
    ) -> PrepareResult:
        """Return a tuple of (payload, infos, subdir).

        - `payload`: dict to be written (before infos wrapping)
        - `infos`: optional metadata dict to be wrapped under `__infos__`
        - `subdir`: optional absolute directory where the daily file should be written
        """
        pass

    def write(
        self,
        key: str,
        data: dict,
        *,
        reason: str | None = None,
        retries: int | None = None,
    ) -> Path:
        result = self.prepare(key, data, reason, retries)
        payload, infos, subdir = result.payload, result.infos, result.subdir
        if infos:
            payload = DailyJsonlWriter.wrap_with_infos(payload, infos)
        return self.writer.write(self.prefix, payload, subdir=subdir)

    def record(
        self,
        key: str,
        data: dict,
        *,
        reason: str | None = None,
        retries: int | None = None,
    ) -> Path:
        """Unified recording API: write then log via a private helper."""
        path = self.write(key, data, reason=reason, retries=retries)
        self._log_record(key, path, reason=reason, retries=retries)
        return path

    def _log_record(
        self,
        key: str,
        path: Path,
        *,
        reason: str | None = None,
        retries: int | None = None,
    ) -> None:
        """Log a concise record message using `LOG_KIND`/`LOG_LEVEL`.

        Includes optional `reason` and `retries` context when provided.
        """
        kind = getattr(self, "LOG_KIND", self.__class__.__name__)
        level = getattr(self, "LOG_LEVEL", logging.INFO)
        parts: list[str] = []
        if retries is not None:
            parts.append(f"retries={retries}")
        if reason is not None:
            parts.append(f"reason={reason}")
        suffix = f" ({', '.join(parts)})" if parts else ""
        logging.log(level, f"{kind}: recorded {key} to {path}{suffix}")
