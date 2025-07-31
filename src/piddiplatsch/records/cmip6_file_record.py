import logging
from functools import cached_property
from pathlib import PurePosixPath
from typing import Any

from piddiplatsch.config import config
from piddiplatsch.models import CMIP6FileModel
from piddiplatsch.records.base import BaseRecord
from piddiplatsch.utils.pid import asset_pid, item_pid


class CMIP6FileRecord(BaseRecord):
    """Wraps a CMIP6 STAC asset and prepares Handle record for a file."""

    def __init__(self, item: dict[str, Any], asset_key: str, strict: bool):
        super().__init__(item, strict=strict)
        self.asset_key = asset_key
        self.asset = self._get_asset(self.item, self.asset_key)

    @staticmethod
    def _get_asset(item: dict[str, Any], asset_key: str) -> dict[str, Any]:
        try:
            return item["assets"][asset_key]
        except KeyError as e:
            logging.error(f"Missing asset '{asset_key}' in item: {e}")
            raise ValueError(f"Asset key '{asset_key}' not found") from e

    @staticmethod
    def _build_handle(pid: str) -> str:
        prefix = config.get("handle", "prefix")
        return f"{prefix}/{pid}"

    def _extract_parent_pid(self) -> str:
        try:
            return item_pid(self.item["id"])
        except KeyError as e:
            logging.error("Missing 'id' in item for parent PID: %s", e)
            raise ValueError("Missing required 'id' field") from e

    def _extract_pid(self) -> str:
        try:
            return asset_pid(self.item["id"], self.asset_key)
        except KeyError as e:
            logging.error("Missing 'id' in item for asset PID: %s", e)
            raise ValueError("Missing required 'id' field") from e

    def _extract_url(self) -> str:
        prefix = config.get("handle", {}).get("prefix", "")
        lp_url = config.get("cmip6", {}).get("landing_page_url", "")
        return f"{lp_url}/{prefix}/{self.pid}"

    def _extract_download_url(self) -> str:
        try:
            return self.asset["href"]
        except KeyError as e:
            logging.error(f"Missing 'href' in asset: {e}")
            raise ValueError("Missing required 'href' field in asset") from e

    def _extract_filename(self) -> str:
        try:
            return PurePosixPath(self.asset["href"]).name
        except KeyError as e:
            logging.error(f"Missing 'href' in asset: {e}")
            raise ValueError("Missing required 'href' field in asset") from e

    def _extract_checksum(self) -> str | None:
        return self.asset.get("checksum")

    def _extract_size(self) -> int | None:
        try:
            return int(self.asset["size"])
        except (KeyError, ValueError, TypeError):
            logging.debug("Size not available or invalid in asset")
            return None

    @cached_property
    def parent(self) -> str:
        return self._build_handle(self._extract_parent_pid())

    @cached_property
    def pid(self) -> str:
        return self._extract_pid()

    @cached_property
    def url(self) -> str:
        return self._extract_url()

    @cached_property
    def filename(self) -> str:
        return self._extract_filename()

    @cached_property
    def checksum(self) -> str | None:
        return self._extract_checksum()

    @cached_property
    def size(self) -> int | None:
        return self._extract_size()

    @cached_property
    def download_url(self) -> str:
        return self._extract_download_url()

    def as_handle_model(self) -> CMIP6FileModel:
        return CMIP6FileModel(
            URL=self.url,
            AGGREGATION_LEVEL="FILE",
            IS_PART_OF=self.parent,
            FILE_NAME=self.filename,
            CHECKSUM=self.checksum,
            FILE_SIZE=self.size,
            DOWNLOAD_URL=self.download_url,
        )


def extract_asset_records(
    item: dict[str, Any], exclude_keys: list[str], strict: bool
) -> list[CMIP6FileRecord]:
    """Return a list of CMIP6FileRecord instances for all assets except those in exclude_keys."""
    exclude_keys = set(exclude_keys or [])
    assets = item.get("assets", {})

    records = []
    for key in assets:
        if key in exclude_keys:
            continue
        try:
            records.append(CMIP6FileRecord(item, key, strict))
        except ValueError as e:
            logging.warning(f"Skipping asset '{key}': {e}")
    return records
