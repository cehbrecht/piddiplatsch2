import logging
from typing import Any, Dict, Optional
from uuid import uuid3, NAMESPACE_URL
from pathlib import PurePosixPath

from piddiplatsch.models import CMIP6AssetModel


class CMIP6AssetRecord:
    """Wraps a CMIP6 STAC asset and prepares Handle record for a file."""

    def __init__(self, item: Dict[str, Any], asset_key: str):
        self.item = item
        self.asset_key = asset_key
        self.asset = self._get_asset(item, asset_key)

        self._pid = self._extract_pid(item, asset_key)
        self._url = self._extract_url(self.asset)
        self._filename = self._extract_filename(self._url)
        self._checksum = self._extract_checksum(self.asset)
        self._size = self._extract_size(self.asset)

    @staticmethod
    def _get_asset(item: Dict[str, Any], asset_key: str) -> Dict[str, Any]:
        try:
            return item["assets"][asset_key]
        except KeyError as e:
            logging.error(f"Missing asset '{asset_key}' in item: {e}")
            raise ValueError(f"Asset key '{asset_key}' not found") from e

    @staticmethod
    def _extract_pid(item: Dict[str, Any], asset_key: str) -> Any:
        # TODO: use correct PID
        id_str = item.get("id", "") + "#" + asset_key
        return uuid3(NAMESPACE_URL, id_str)

    @staticmethod
    def _extract_url(asset: Dict[str, Any]) -> str:
        try:
            return asset["href"]
        except KeyError as e:
            logging.error(f"Missing 'href' in asset: {e}")
            raise ValueError("Missing required 'href' field in asset") from e

    @staticmethod
    def _extract_filename(href: str) -> str:
        return PurePosixPath(href).name

    @staticmethod
    def _extract_checksum(asset: Dict[str, Any]) -> Optional[str]:
        return asset.get("checksum")

    @staticmethod
    def _extract_size(asset: Dict[str, Any]) -> Optional[int]:
        try:
            return int(asset["size"])
        except (KeyError, ValueError, TypeError):
            logging.debug("Size not available or invalid in asset")
            return None

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
    def checksum(self) -> Optional[str]:
        return self._checksum

    @property
    def size(self) -> Optional[int]:
        return self._size

    def as_handle_model(self) -> CMIP6AssetModel:
        return CMIP6AssetModel(
            # PID=self.pid,
            URL=self.url,
            AGGREGATION_LEVEL="File",
            FILENAME=self.filename,
            CHECKSUM=self.checksum,
            SIZE=self.size,
        )

    def as_record(self) -> dict:
        return self.as_handle_model().model_dump()

    def as_json(self) -> str:
        return self.as_handle_model().model_dump_json()
