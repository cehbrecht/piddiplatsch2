import logging
from datetime import datetime, timezone
from typing import Any

from dateutil.parser import isoparse
from jsonschema import ValidationError, validate

from piddiplatsch.config import config
from piddiplatsch.models import CMIP6DatasetModel, HostingNode
from piddiplatsch.records.base import BaseRecord
from piddiplatsch.schema import CMIP6_SCHEMA as SCHEMA
from piddiplatsch.utils.pid import asset_pid, item_pid


class CMIP6DatasetRecord(BaseRecord):
    """Wraps a validated CMIP6 STAC item and prepares Handle records."""

    def __init__(self, item: dict[str, Any], strict: bool, exclude_keys: list[str]):
        self.item = item
        self.strict = strict
        self.exclude_keys = exclude_keys

        # config
        self.prefix = config.get("handle", {}).get("prefix", "")
        self.lp_url = config.get("cmip6", {}).get("landing_page_url", "")
        self.max_parts = config.get("cmip6", {}).get("max_parts", -1)

        # Validate the STAC item against schema
        try:
            validate(instance=self.item, schema=SCHEMA)
        except ValidationError as e:
            logging.error(
                "Schema validation failed at %s: %s", list(e.absolute_path), e.message
            )
            raise ValueError(f"Invalid CMIP6 STAC item: {e.message}") from e

        self._pid = self._extract_pid(self.item)
        self._url = self._extract_url()
        self._is_part_of = self._extract_is_part_of(self.item)
        self._has_parts = self._extract_has_parts(self.item)
        self._dataset_id = self._extract_dataset_id(self.item)
        self._dataset_version = self._extract_dataset_version(self.item)
        self._hosting_node = self._extract_hosting_node(self.item)
        self._replica_nodes = self._extract_replica_nodes(self.item)
        self._unpublished_replicas = self._extract_unpublished_replicas(self.item)
        self._unpublished_hosts = self._extract_unpublished_host(self.item)

    @staticmethod
    def _extract_pid(item: dict[str, Any]) -> Any:
        try:
            return item_pid(item["id"])
        except KeyError as e:
            logging.error("Missing 'id' in item: %s", e)
            raise ValueError("Missing required 'id' field") from e

    def _extract_url(self) -> str:
        return f"{self.lp_url}/{self.prefix}/{self.pid}"

    @staticmethod
    def _extract_dataset_id(item: dict[str, Any]) -> str:
        id_str = item.get("id", "")
        parts = id_str.rsplit(".", 1)
        if not parts or len(parts) < 2:
            logging.warning(f"Unable to parse dataset ID from: {id_str}")
        return parts[0]

    @staticmethod
    def _extract_dataset_version(item: dict[str, Any]) -> str:
        id_str = item.get("id", "")
        parts = id_str.rsplit(".", 1)
        if len(parts) < 2:
            logging.warning(f"No version found in ID: {id_str}")
            return ""
        return parts[1]

    def _extract_has_parts(self, item: dict[str, Any]) -> list[str]:
        parts = []
        item_id = item.get("id")
        if not item_id:
            logging.warning("Missing item 'id'; cannot compute HAS_PARTS")
            return parts

        asset_keys = item.get("assets", {}).keys()
        for key in asset_keys:
            if key in self.exclude_keys:
                continue
            if self.max_parts > -1 and len(parts) >= self.max_parts:
                logging.debug(f"Reached limit of {self.max_parts} assets.")
                break
            parts.append(asset_pid(item_id, key))
        return parts

    @staticmethod
    def _extract_is_part_of(item: dict[str, Any]) -> str | None:
        # Placeholder; return None or derive from parent ID if needed
        return None

    @staticmethod
    def _parse_datetime(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            return isoparse(value)
        except Exception:
            logging.warning(f"Failed to parse datetime: {value}")
            return None

    @staticmethod
    def _extract_hosting_node(item: dict[str, Any]) -> HostingNode:
        assets = item.get("assets", {})
        ref_node = assets.get("reference_file", {}).get("alternate:name")
        data_node = assets.get("data0001", {}).get("alternate:name")
        host = ref_node or data_node or "unknown"

        pub_on = None
        for key in ["reference_file", "data0001"]:
            published = assets.get(key, {}).get("published_on")
            if published:
                pub_on = published
                break

        if not pub_on:
            pub_on = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        return HostingNode(
            host=host, published_on=CMIP6DatasetRecord._parse_datetime(pub_on)
        )

    @staticmethod
    def _extract_replica_nodes(item: dict[str, Any]) -> list[HostingNode]:
        nodes = []
        locations = item.get("locations", {}).get("location", [])
        if isinstance(locations, dict):
            locations = [locations]
        for loc in locations:
            host = loc.get("host")
            pub_on = CMIP6DatasetRecord._parse_datetime(loc.get("publishedOn"))
            if host:
                nodes.append(HostingNode(host=host, published_on=pub_on))
        return nodes

    @staticmethod
    def _extract_unpublished_host(item: dict[str, Any]) -> HostingNode:
        host = item.get("unpublished_hosts", {}).get("host", "unknown")
        pub_on = item.get("unpublished_hosts", {}).get("published_on", "")
        return HostingNode(
            host=host, published_on=CMIP6DatasetRecord._parse_datetime(pub_on)
        )

    @staticmethod
    def _extract_unpublished_replicas(item: dict[str, Any]) -> list[HostingNode]:
        replicas = []
        data = item.get("unpublished_replicas", [])
        if isinstance(data, dict):
            data = [data]
        for entry in data:
            host = entry.get("host", "unknown")
            pub_on = CMIP6DatasetRecord._parse_datetime(entry.get("published_on", ""))
            replicas.append(HostingNode(host=host, published_on=pub_on))
        return replicas

    @property
    def pid(self) -> Any:
        return self._pid

    @property
    def url(self) -> str:
        return self._url

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
    def unpublished_replicas(self) -> list[str]:
        return self._unpublished_replicas

    @property
    def unpublished_hosts(self) -> list[str]:
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
