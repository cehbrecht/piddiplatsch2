import logging
from abc import ABC, abstractmethod
from pathlib import Path

from piddiplatsch.persist.helpers import DailyJsonlWriter


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

    def record(
        self,
        key: str,
        data: dict,
        *,
        reason: str | None = None,
        retries: int | None = None,
    ) -> Path:
        """Unified recording API with generalized logging.

        Delegates to `write()` and logs a concise message including the
        recorder type (class name or `LOG_KIND`), target path, and optional
        `reason`/`retries` details.
        """
        path = self.write(key, data, reason=reason, retries=retries)

        # Determine kind and level for logging
        kind = getattr(self, "LOG_KIND", self.__class__.__name__)
        level = getattr(self, "LOG_LEVEL", logging.INFO)

        parts: list[str] = []
        if retries is not None:
            parts.append(f"retries={retries}")
        if reason is not None:
            parts.append(f"reason={reason}")
        suffix = f" ({', '.join(parts)})" if parts else ""

        logging.log(level, f"{kind}: recorded {key} to {path}{suffix}")
        return path
