import logging
from functools import cached_property
from typing import Any

from piddiplatsch.config import config
from piddiplatsch.exceptions import LookupError
from piddiplatsch.lookup import get_lookup
from piddiplatsch.models import CMIP6DatasetModel, HostingNode
from piddiplatsch.monitoring import stats
from piddiplatsch.records.base import BaseCMIP6Record
from piddiplatsch.utils.models import (
    asset_pid,
    build_handle,
    item_pid,
    parse_datetime,
    parse_pid,
)
from piddiplatsch.utils.stac import Properties, split_cmip6_id

PREFERRED_KEYS = ("reference_file", "data0000", "data0001")


class CMIP6DatasetRecord(BaseCMIP6Record):
    """Wraps a validated CMIP6 STAC item and prepares Handle records."""

    def __init__(
        self,
        item: dict[str, Any],
        exclude_keys: list[str] | None = None,
        additional_attributes: dict[str, Any] | None = None,
    ):
        super().__init__(item, additional_attributes)
        self.exclude_keys = set(exclude_keys or [])
        self.max_parts = config.get("cmip6", {}).get("max_parts", -1)
        self.lookup = get_lookup()

    @cached_property
    def dataset_properties(self) -> Properties:
        return split_cmip6_id(self.item_id)

    @cached_property
    def pid(self) -> str:
        pid_ = parse_pid(self.get_property("pid"))
        if not pid_:
            pid_ = item_pid(self.item_id)
            logging.warning(
                f"Creating new dataset pid: pid={pid_}, ds_id={self.item_id}"
            )
        else:
            logging.info(
                f"Using existing dataset pid: pid={pid_}, ds_id={self.item_id}"
            )
        return pid_

    @cached_property
    def dataset_id(self) -> str:
        parts = self.item_id.rsplit(".", 1)
        return parts[0] if len(parts) > 1 else self.item_id

    @cached_property
    def dataset_version(self) -> str:
        parts = self.item_id.rsplit(".", 1)
        return parts[1] if len(parts) > 1 else ""

    @cached_property
    def has_parts(self) -> list[str]:
        parts = []
        for key in self.assets.keys():
            if key in self.exclude_keys:
                continue
            if self.max_parts > -1 and len(parts) >= self.max_parts:
                logging.warning(f"Reached limit of {self.max_parts} assets.")
                break
            pid = asset_pid(self.item_id, key)
            handle = build_handle(pid, as_uri=True)
            parts.append(handle)
        return parts

    @cached_property
    def is_part_of(self) -> str | None:
        # Placeholder - can be updated to extract actual parent PID if needed
        return None

    @cached_property
    def host(self) -> str:
        for key in PREFERRED_KEYS:
            host = self.get_asset_property(key, "alternate:name")
            if host:
                return host
        return "unknown"

    @cached_property
    def published_on(self) -> str:
        for key in PREFERRED_KEYS:
            published_on = self.get_asset_property(key, "published_on")
            if published_on:
                return parse_datetime(published_on)
        return parse_datetime(self.default_publication_time)

    @cached_property
    def hosting_node(self) -> HostingNode:
        return HostingNode(host=self.host, published_on=self.published_on)

    @cached_property
    def replica_nodes(self) -> list[HostingNode]:
        nodes = []
        known_hosts = set()
        for key in PREFERRED_KEYS:
            alternates = self.get_asset_property(key, "alternate", {})
            for host, values in alternates.items():
                published_on = values.get("published_on") or self.published_on
                if host not in known_hosts:
                    known_hosts.add(host)
                    nodes.append(HostingNode(host=host, published_on=published_on))
        return nodes

    @cached_property
    def retracted(self) -> bool:
        raw = str(self.item.get("properties", {}).get("retracted", "false"))
        return raw.strip().lower() in ("true", "1", "yes")

    @cached_property
    def retracted_on(self) -> bool:
        if self.retracted:
            return parse_datetime(self.default_publication_time)
        return None

    @cached_property
    def previous_version(self) -> str | None:
        """Return the previous version of this dataset.

        Raises:
            LookupError: If the lookup fails.
        """
        try:
            # Backend-agnostic: lookup handles item_id internally
            item_ids = self.lookup.find_versions(self.item_id)
        except LookupError as e:
            logging.error(f"Failed to fetch versions for {self.dataset_id}: {e}")
            raise

        if not item_ids:
            logging.info(f"No versions found for id={self.dataset_id}")
            return None

        current_version = self.dataset_properties.version_number
        for item_id in item_ids:
            version = split_cmip6_id(item_id).version_number
            if version < current_version:
                return item_id

        logging.info(
            f"Dataset id={self.dataset_id} is the latest version {current_version}"
        )
        return None

    def as_handle_model(self) -> CMIP6DatasetModel:
        dsm = CMIP6DatasetModel(
            URL=self.url,
            DATASET_ID=self.dataset_id,
            DATASET_VERSION=self.dataset_version,
            PREVIOUS_VERSION=self.previous_version,
            HAS_PARTS=self.has_parts,
            IS_PART_OF=self.is_part_of,
            HOSTING_NODE=self.hosting_node,
            REPLICA_NODES=self.replica_nodes,
            _RETRACTED=self.retracted,
            RETRACTED_ON=self.retracted_on,
        )

        if self.previous_version:
            logging.info(
                f"Dataset id={self.dataset_id} has previous version {self.previous_version}"
            )

        if self.retracted:
            stats.retracted(f"Dataset id={self.dataset_id} is retracted!")

        if self.replica_nodes:
            stats.replica(
                f"Dataset id={self.dataset_id} has {len(self.replica_nodes)} replica nodes"
            )

        dsm.set_pid(self.pid)
        return dsm
