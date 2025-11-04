import logging
from functools import cached_property
from pathlib import PurePosixPath
from typing import Any

from piddiplatsch.models import CMIP6FileModel
from piddiplatsch.records.base import BaseCMIP6Record
from piddiplatsch.utils.models import asset_pid, build_handle, item_pid, parse_pid, parse_multihash_checksum


class CMIP6FileRecord(BaseCMIP6Record):
    """Wraps a CMIP6 STAC asset and prepares Handle record for a file."""

    def __init__(self, item: dict[str, Any], asset_key: str, strict: bool):
        super().__init__(item, strict=strict)
        self.asset_key = asset_key

    @cached_property
    def asset(self) -> dict[str, Any]:
        return self.get_asset(self.asset_key)

    @cached_property
    def alternates(self) -> dict[str, Any]:
        return self.asset.get("alternate", {})

    def get_value(self, key: str) -> Any:
        value = self.asset.get(key, "")
        # check alternates if not found
        if not value and self.alternates:
            # pick first available
            for alt in self.alternates.values():
                if alt.get(key):
                    value = alt[key]
                    break
        return value

    @cached_property
    def tracking_id(self) -> str:
        return self.asset.get("tracking_id")

    @cached_property
    def pid(self) -> str:
        pid_ = parse_pid(self.tracking_id)
        if not pid_:
            pid_ = asset_pid(self.item_id, self.asset_key)
            logging.warning(
                f"Creating new file pid: pid={pid_}, asset={self.asset_key}"
            )
        else:
            logging.warning(
                f"Using existing file pid: pid={pid_}, asset={self.asset_key}"
            )
        return pid_

    @cached_property
    def parent(self) -> str:
        return build_handle(item_pid(self.item_id), as_uri=True)

    @cached_property
    def href(self) -> str:
        """Resolved asset URL."""
        return self.get_value("href")

    @cached_property
    def download_url(self) -> str:
        """Alias for href to keep external API consistent."""
        return self.href

    @cached_property
    def replica_download_urls(self) -> list[str]:
        urls = set()
        for alt in self.alternates.values():
            if alt.get("href"):
                urls.add(alt["href"])
        return list(urls)

    @cached_property
    def filename(self) -> str:
        """
        Filename derived from the resolved href.
        """
        # Try filename from resolved href first
        resolved_name = PurePosixPath(self.href).name
        return resolved_name
    
    @cached_property
    def checksum_with_method(self) -> str | None:
        cval = self.get_value("file:checksum")
        try:
            cmethod, chex = parse_multihash_checksum(cval)
            value = f"{cmethod}:{chex}"
        except Exception as err:
           value = f"unknown:{cval}"
        return value

    @cached_property
    def checksum(self) -> str | None:
        try:
            chex = self.checksum_with_method.split(":")[1]
        except Exception as err:
            chex = None
        return chex

    @cached_property
    def checksum_method(self) -> str | None:
        try:
            cmethod = self.checksum_with_method.split(":")[0]
        except Exception as err:
            cmethod = None
        return cmethod

    @cached_property
    def size(self) -> int | None:
        try:
            return int(self.get_value("file:size"))
        except (ValueError, TypeError):
            return None

    def as_handle_model(self) -> CMIP6FileModel:
        fm = CMIP6FileModel(
            URL=self.url,
            AGGREGATION_LEVEL="FILE",
            IS_PART_OF=self.parent,
            FILE_NAME=self.filename,
            CHECKSUM=self.checksum,
            CHECKSUM_METHOD=self.checksum_method,
            FILE_SIZE=self.size,
            DOWNLOAD_URL=self.download_url,
            REPLICA_DOWNLOAD_URLS=self.replica_download_urls,
        )
        fm.set_pid(self.pid)
        return fm


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
            record = CMIP6FileRecord(item, key, strict)
            if strict:
                record.validate()
            records.append(record)
        except ValueError as e:
            logging.warning(f"Skipping asset '{key}': {e}")
    return records
