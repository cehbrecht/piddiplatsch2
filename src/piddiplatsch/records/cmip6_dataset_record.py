import logging
from datetime import datetime, timezone
from functools import cached_property
from typing import Any

from piddiplatsch.config import config
from piddiplatsch.models import CMIP6DatasetModel, HostingNode
from piddiplatsch.records.base import BaseCMIP6Record
from piddiplatsch.records.utils import parse_datetime
from piddiplatsch.utils.pid import asset_pid, item_pid


class CMIP6DatasetRecord(BaseCMIP6Record):
    """Wraps a validated CMIP6 STAC item and prepares Handle records."""

    def __init__(
        self,
        item: dict[str, Any],
        strict: bool,
        exclude_keys: list[str] | None = None,
    ):
        super().__init__(item, strict=strict)
        self.exclude_keys = set(exclude_keys or [])
        self.max_parts = config.get("cmip6", {}).get("max_parts", -1)

    @cached_property
    def pid(self) -> str:
        return item_pid(self.item_id)

    @cached_property
    def dataset_id(self) -> str:
        id_str = self.item.get("id", "")
        parts = id_str.rsplit(".", 1)
        if len(parts) < 2:
            logging.warning(f"Unable to parse dataset ID from: {id_str}")
            return id_str
        return parts[0]

    @cached_property
    def dataset_version(self) -> str:
        id_str = self.item.get("id", "")
        parts = id_str.rsplit(".", 1)
        if len(parts) < 2:
            logging.warning(f"No version found in ID: {id_str}")
            return ""
        return parts[1]

    @cached_property
    def has_parts(self) -> list[str]:
        parts = []
        item_id = self.item.get("id")
        if not item_id:
            logging.warning("Missing item 'id'; cannot compute HAS_PARTS")
            return parts

        for key in self.item.get("assets", {}).keys():
            if key in self.exclude_keys:
                continue
            if self.max_parts > -1 and len(parts) >= self.max_parts:
                logging.debug(f"Reached limit of {self.max_parts} assets.")
                break
            parts.append(asset_pid(item_id, key))
        return parts

    @cached_property
    def is_part_of(self) -> str | None:
        # Placeholder - can be updated to extract actual parent PID if needed
        return None

    @cached_property
    def hosting_node(self) -> HostingNode:
        assets = self.item.get("assets", {})
        ref_node = assets.get("reference_file", {}).get("alternate:name")
        data_node = assets.get("data0001", {}).get("alternate:name")
        host = ref_node or data_node or "unknown"

        published_on = None
        for key in ("reference_file", "data0001"):
            published_on = assets.get(key, {}).get("published_on")
            if published_on:
                break

        if not published_on:
            published_on = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        return HostingNode(host=host, published_on=parse_datetime(published_on))

    @cached_property
    def replica_nodes(self) -> list[HostingNode]:
        nodes = []
        locations = self.item.get("locations", {}).get("location", [])
        if isinstance(locations, dict):
            locations = [locations]
        for loc in locations:
            host = loc.get("host")
            pub_on = parse_datetime(loc.get("publishedOn"))
            if host:
                nodes.append(HostingNode(host=host, published_on=pub_on))
        return nodes

    @cached_property
    def unpublished_hosts(self) -> HostingNode:
        unpublished = self.item.get("unpublished_hosts", {})
        host = unpublished.get("host", "unknown")
        pub_on = unpublished.get("published_on", "")
        return HostingNode(host=host, published_on=parse_datetime(pub_on))

    @cached_property
    def unpublished_replicas(self) -> list[HostingNode]:
        replicas = []
        data = self.item.get("unpublished_replicas", [])
        if isinstance(data, dict):
            data = [data]
        for entry in data:
            host = entry.get("host", "unknown")
            pub_on = parse_datetime(entry.get("published_on", ""))
            replicas.append(HostingNode(host=host, published_on=pub_on))
        return replicas

    def as_handle_model(self) -> CMIP6DatasetModel:
        return CMIP6DatasetModel(
            URL=self.url,
            DATASET_ID=self.dataset_id,
            DATASET_VERSION=self.dataset_version,
            HAS_PARTS=self.has_parts,
            IS_PART_OF=self.is_part_of,
            HOSTING_NODE=self.hosting_node,
            REPLICA_NODES=self.replica_nodes,
            UNPUBLISHED_REPLICAS=self.unpublished_replicas,
            UNPUBLISHED_HOSTS=self.unpublished_hosts,
        )
