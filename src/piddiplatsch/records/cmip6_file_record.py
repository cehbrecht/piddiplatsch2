import logging
from functools import cached_property
from pathlib import PurePosixPath
from typing import Any

from piddiplatsch.models import CMIP6FileModel
from piddiplatsch.records.base import BaseCMIP6Record
from piddiplatsch.utils.pid import asset_pid, build_handle, item_pid


class CMIP6FileRecord(BaseCMIP6Record):
    """Wraps a CMIP6 STAC asset and prepares Handle record for a file."""

    def __init__(self, item: dict[str, Any], asset_key: str, strict: bool):
        super().__init__(item, strict=strict)
        self.asset_key = asset_key

    @cached_property
    def asset(self) -> dict[str, Any]:
        try:
            return self.item["assets"][self.asset_key]
        except KeyError as e:
            logging.error(f"Missing asset '{self.asset_key}' in item: {e}")
            raise ValueError(f"Asset key '{self.asset_key}' not found") from e

    @cached_property
    def item_id(self) -> str:
        try:
            return self.item["id"]
        except KeyError as e:
            logging.error("Missing 'id' in item: %s", e)
            raise ValueError("Missing required 'id' field") from e

    @cached_property
    def pid(self) -> str:
        return asset_pid(self.item_id, self.asset_key)

    @cached_property
    def parent(self) -> str:
        return build_handle(item_pid(self.item_id))

    @cached_property
    def filename(self) -> str:
        try:
            return PurePosixPath(self.asset["href"]).name
        except KeyError as e:
            logging.error(f"Missing 'href' in asset: {e}")
            raise ValueError("Missing required 'href' field in asset") from e

    @cached_property
    def checksum(self) -> str | None:
        return self.asset.get("checksum")

    @cached_property
    def size(self) -> int | None:
        try:
            return int(self.asset["size"])
        except (KeyError, ValueError, TypeError):
            logging.debug("Size not available or invalid in asset")
            return None

    @cached_property
    def download_url(self) -> str:
        try:
            return self.asset["href"]
        except KeyError as e:
            logging.error(f"Missing 'href' in asset: {e}")
            raise ValueError("Missing required 'href' field in asset") from e

    def as_handle_model(self) -> CMIP6FileModel:
        return CMIP6FileModel(
            URL=self.url,  # from BaseCMIP6Record using self.pid
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
    """Given a CMIP6 STAC item, return a list of CMIP6FileRecord instances
    for all asset keys except those in exclude_keys.
    """
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
