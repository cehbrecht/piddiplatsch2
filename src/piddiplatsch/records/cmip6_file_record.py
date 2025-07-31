import logging
from pathlib import PurePosixPath
from typing import Any

from piddiplatsch.config import config
from piddiplatsch.models import CMIP6FileModel
from piddiplatsch.records.base import BaseRecord
from piddiplatsch.utils.pid import asset_pid, item_pid


class CMIP6FileRecord(BaseRecord):
    """Wraps a CMIP6 STAC asset and prepares Handle record for a file."""

    def __init__(self, item: dict[str, Any], asset_key: str):
        self.item = item
        self.asset_key = asset_key
        self.asset = self._get_asset(item, asset_key)

        # config
        self.prefix = config.get("handle", {}).get("prefix", "")
        self.lp_url = config.get("cmip6", {}).get("landing_page_url", "")

        self._parent = self._build_handle(self._extract_parent_pid(item))
        self._pid = self._extract_pid(item, asset_key)
        self._url = self._extract_url()
        self._filename = self._extract_filename(self.asset)
        self._checksum = self._extract_checksum(self.asset)
        self._size = self._extract_size(self.asset)
        self._download_url = self._extract_download_url(self.asset)

    @staticmethod
    def _get_asset(item: dict[str, Any], asset_key: str) -> dict[str, Any]:
        try:
            return item["assets"][asset_key]
        except KeyError as e:
            logging.error(f"Missing asset '{asset_key}' in item: {e}")
            raise ValueError(f"Asset key '{asset_key}' not found") from e

    @staticmethod
    def _build_handle(pid):
        """Build a full handle by combining the prefix and the PID."""
        # TODO: duplicate of handle_client.build_handle
        prefix = config.get("handle", "prefix")
        return f"{prefix}/{pid}"

    @staticmethod
    def _extract_parent_pid(item: dict[str, Any]) -> str:
        try:
            return item_pid(item["id"])
        except KeyError as e:
            logging.error("Missing 'id' in item for parent PID: %s", e)
            raise ValueError("Missing required 'id' field") from e

    @staticmethod
    def _extract_pid(item: dict[str, Any], asset_key: str) -> str:
        try:
            return asset_pid(item["id"], asset_key)
        except KeyError as e:
            logging.error("Missing 'id' in item for asset PID: %s", e)
            raise ValueError("Missing required 'id' field") from e

    def _extract_url(self) -> str:
        url = f"{self.lp_url}/{self.prefix}/{self.pid}"
        return url

    @staticmethod
    def _extract_download_url(asset: dict[str, Any]) -> str:
        try:
            return asset["href"]
        except KeyError as e:
            logging.error(f"Missing 'href' in asset: {e}")
            raise ValueError("Missing required 'href' field in asset") from e

    @staticmethod
    def _extract_filename(asset: dict[str, Any]) -> str:
        try:
            href = asset["href"]
            return PurePosixPath(href).name
        except KeyError as e:
            logging.error(f"Missing 'href' in asset: {e}")
            raise ValueError("Missing required 'href' field in asset") from e

    @staticmethod
    def _extract_checksum(asset: dict[str, Any]) -> str | None:
        return asset.get("checksum")

    @staticmethod
    def _extract_size(asset: dict[str, Any]) -> int | None:
        try:
            return int(asset["size"])
        except (KeyError, ValueError, TypeError):
            logging.debug("Size not available or invalid in asset")
            return None

    @property
    def parent(self) -> Any:
        return self._parent

    @property
    def pid(self) -> Any:
        return self._pid

    @property
    def url(self) -> str:
        return self._url

    @property
    def filename(self) -> str:
        return self._filename

    @property
    def checksum(self) -> str | None:
        return self._checksum

    @property
    def size(self) -> int | None:
        return self._size

    @property
    def download_url(self) -> str:
        return self._download_url

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
    item: dict[str, Any], exclude_keys: list[str]
) -> list[CMIP6FileRecord]:
    """Given a CMIP6 STAC item, return a list of CMIP6AssetRecord instances
    for all asset keys except those in exclude_keys.

    Args:
        item: A CMIP6 STAC item as a dict.
        exclude_keys: Optional list of asset keys to ignore (e.g., ["thumbnail", "quicklook"]).

    Returns:
        A list of CMIP6AssetRecord objects.

    """
    exclude_keys = set(exclude_keys or [])
    assets = item.get("assets", {})

    records = []
    for key in assets:
        if key in exclude_keys:
            continue
        try:
            record = CMIP6FileRecord(item, key)
            records.append(record)
        except ValueError as e:
            # Log and skip problematic assets
            logging.warning(f"Skipping asset '{key}': {e}")
    return records
