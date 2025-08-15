import logging
from functools import cached_property
from pathlib import PurePosixPath
from typing import Any

from piddiplatsch.models import CMIP6FileModel
from piddiplatsch.records.base import BaseCMIP6Record
from piddiplatsch.records.utils import parse_pid
from piddiplatsch.utils.pid import asset_pid, build_handle, item_pid


class CMIP6FileRecord(BaseCMIP6Record):
    """Wraps a CMIP6 STAC asset and prepares Handle record for a file."""

    def __init__(self, item: dict[str, Any], asset_key: str, strict: bool):
        super().__init__(item, strict=strict)
        self.asset_key = asset_key

    @cached_property
    def asset(self) -> dict[str, Any]:
        return self.get_asset(self.asset_key)

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
        """Resolved asset URL, preferring alternates when present."""
        alternates = self.asset.get("alternate", {})
        if alternates:
            preferred_key = "ceda.ac.uk"
            if preferred_key in alternates:
                alt_href = alternates[preferred_key].get("href")
                if alt_href:
                    return alt_href
            # Otherwise, pick first available
            for alt in alternates.values():
                if alt.get("href"):
                    return alt["href"]

        # Fallback: main href
        return self.asset.get("href", "")

    @cached_property
    def download_url(self) -> str:
        """Alias for href to keep external API consistent."""
        return self.href

    @cached_property
    def filename(self) -> str:
        """
        Filename derived from the resolved href,
        falling back to the main href if alternate lacks a filename.
        """
        # Try filename from resolved href first
        resolved_name = PurePosixPath(self.href).name
        if resolved_name:
            return resolved_name

        # Fallback: derive from main href
        return PurePosixPath(self.asset.get("href", "")).name

    @cached_property
    def checksum(self) -> str | None:
        return self.asset.get("file:checksum")

    @cached_property
    def size(self) -> int | None:
        try:
            return int(self.asset.get("file:size"))
        except (ValueError, TypeError):
            return None

    def as_handle_model(self) -> CMIP6FileModel:
        fm = CMIP6FileModel(
            URL=self.url,
            AGGREGATION_LEVEL="FILE",
            IS_PART_OF=self.parent,
            FILE_NAME=self.filename,
            CHECKSUM=self.checksum,
            FILE_SIZE=self.size,
            DOWNLOAD_URL=self.download_url,
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
