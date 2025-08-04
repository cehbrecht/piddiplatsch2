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

        # Config
        self.max_parts = config.get("cmip6", {}).get("max_parts", -1)

        # Precompute properties
        self._is_part_of = self._extract_is_part_of()
        self._has_parts = self._extract_has_parts()
        self._dataset_id = self._extract_dataset_id()
        self._dataset_version = self._extract_dataset_version()
        self._hosting_node = self._extract_hosting_node()
        self._replica_nodes = self._extract_replica_nodes()
        self._unpublished_replicas = self._extract_unpublished_replicas()
        self._unpublished_hosts = self._extract_unpublished_hosts()

    @cached_property
    def pid(self) -> str:
        try:
            return item_pid(self.item["id"])
        except KeyError as e:
            logging.error("Missing 'id' in item: %s", e)
            raise ValueError("Missing required 'id' field") from e

    def _extract_dataset_id(self) -> str:
        id_str = self.item.get("id", "")
        parts = id_str.rsplit(".", 1)
        if len(parts) < 2:
            logging.warning(f"Unable to parse dataset ID from: {id_str}")
            return id_str  # fallback to full id
        return parts[0]

    def _extract_dataset_version(self) -> str:
        id_str = self.item.get("id", "")
        parts = id_str.rsplit(".", 1)
        if len(parts) < 2:
            logging.warning(f"No version found in ID: {id_str}")
            return ""
        return parts[1]

    def _extract_has_parts(self) -> list[str]:
        parts = []
        item_id = self.item.get("id")
        if not item_id:
            logging.warning("Missing item 'id'; cannot compute HAS_PARTS")
            return parts

        assets = self.item.get("assets", {})
        for key in assets:
            if key in self.exclude_keys:
                continue
            if self.max_parts > -1 and len(parts) >= self.max_parts:
                logging.debug(f"Reached limit of {self.max_parts} assets.")
                break
            parts.append(asset_pid(item_id, key))
        return parts

    def _extract_is_part_of(self) -> str | None:
        # Placeholder - could be implemented to extract parent dataset PID if available
        return None

    def _extract_hosting_node(self) -> HostingNode:
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
            # fallback to current UTC datetime string
            published_on = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        return HostingNode(host=host, published_on=parse_datetime(published_on))

    def _extract_replica_nodes(self) -> list[HostingNode]:
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

    def _extract_unpublished_hosts(self) -> HostingNode:
        unpublished = self.item.get("unpublished_hosts", {})
        host = unpublished.get("host", "unknown")
        pub_on = unpublished.get("published_on", "")
        return HostingNode(host=host, published_on=parse_datetime(pub_on))

    def _extract_unpublished_replicas(self) -> list[HostingNode]:
        replicas = []
        data = self.item.get("unpublished_replicas", [])
        if isinstance(data, dict):
            data = [data]
        for entry in data:
            host = entry.get("host", "unknown")
            pub_on = parse_datetime(entry.get("published_on", ""))
            replicas.append(HostingNode(host=host, published_on=pub_on))
        return replicas

    @property
    def dataset_id(self) -> str:
        return self._dataset_id

    @property
    def dataset_version(self) -> str:
        return self._dataset_version

    @property
    def has_parts(self) -> list[str]:
        return self._has_parts

    @property
    def is_part_of(self) -> str | None:
        return self._is_part_of

    @property
    def hosting_node(self) -> HostingNode:
        return self._hosting_node

    @property
    def replica_nodes(self) -> list[HostingNode]:
        return self._replica_nodes

    @property
    def unpublished_replicas(self) -> list[HostingNode]:
        return self._unpublished_replicas

    @property
    def unpublished_hosts(self) -> HostingNode:
        return self._unpublished_hosts

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
