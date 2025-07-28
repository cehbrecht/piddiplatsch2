import logging
from typing import Any, Dict, List, Optional
from uuid import uuid3, NAMESPACE_URL
from datetime import datetime

from jsonschema import validate, ValidationError
from dateutil.parser import isoparse

from piddiplatsch.schema import CMIP6_SCHEMA as SCHEMA
from piddiplatsch.models import CMIP6ItemModel, HostingNode
from piddiplatsch.config import config
from piddiplatsch.utils.pid import item_pid, asset_pid


class CMIP6ItemRecord:
    """Wraps a validated CMIP6 STAC item and prepares Handle records."""

    def __init__(self, item: Dict[str, Any], strict: bool, exclude_keys: List[str]):
        self.item = item
        self.strict = strict
        self.exclude_keys = exclude_keys

        # config
        self.prefix = config.get("handle", {}).get("prefix", "")
        self.lp_url = config.get("cmip6", {}).get("landing_page_url", "")

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
        self._unpublished_replicas = []
        self._unpublished_hosts = self._extract_unpublished(
            self.item, "unpublished_hosts"
        )

    @staticmethod
    def _extract_pid(item: Dict[str, Any]) -> Any:
        try:
            return item_pid(item["id"])
        except KeyError as e:
            logging.error("Missing 'id' in item: %s", e)
            raise ValueError("Missing required 'id' field") from e

    def _extract_url(self) -> str:
        url = f"{self.lp_url}/{self.prefix}/{self.pid}"
        return url

    @staticmethod
    def _extract_stac_url(item: Dict[str, Any]) -> str:
        try:
            return item["links"][0]["href"]
        except (KeyError, IndexError) as e:
            logging.error("Missing 'links[0].href' in item: %s", e)
            raise ValueError("Missing required 'links[0].href' field") from e

    @staticmethod
    def _extract_dataset_id(item: Dict[str, Any]) -> str:
        id_str = item.get("id", "")
        parts = id_str.rsplit(".", 1)
        dataset_id = parts[0]
        return dataset_id

    from piddiplatsch.utils.pid import asset_pid

    def _extract_has_parts(self, item: Dict[str, Any]) -> List[str]:
        parts = []
        item_id = item.get("id")
        if not item_id:
            logging.warning("Missing item 'id'; cannot compute HAS_PARTS")
            return parts

        asset_keys = item.get("assets", {}).keys()
        for key in asset_keys:
            if key in self.exclude_keys:
                continue
            parts.append(asset_pid(item_id, key))

        return parts

    @staticmethod
    def _extract_is_part_of(item: Dict[str, Any]) -> str:
        return None

    @staticmethod
    def _extract_dataset_version(item: Dict[str, Any]) -> str:
        id_str = item.get("id", "")
        parts = id_str.rsplit(".", 1)
        dataset_version = parts[1] if len(parts) > 1 else None
        return dataset_version

    @staticmethod
    def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            return isoparse(value)
        except Exception:
            logging.warning(f"Failed to parse datetime: {value}")
            return None

    @staticmethod
    def _extract_hosting_node(item: Dict[str, Any]) -> HostingNode:
        ref_node = (
            item.get("assets", {}).get("reference_file", {}).get("alternate:name")
        )
        data_node = item.get("assets", {}).get("data0001", {}).get("alternate:name")
        host = ref_node or data_node or "unknown"

        pub_on = None
        for key in ["reference_file", "data0001"]:
            published = item.get("assets", {}).get(key, {}).get("published_on")
            if published:
                pub_on = published
                break

        return HostingNode(
            host=host, published_on=CMIP6ItemRecord._parse_datetime(pub_on)
        )

    @staticmethod
    def _extract_replica_nodes(item: Dict[str, Any]) -> List[HostingNode]:
        nodes = []
        locations = item.get("locations")
        if locations:
            locs = locations.get("location", [])
            if isinstance(locs, dict):
                locs = [locs]
            for loc in locs:
                h = loc.get("host")
                p = CMIP6ItemRecord._parse_datetime(loc.get("publishedOn"))
                if h:
                    nodes.append(HostingNode(host=h, published_on=p))
        return nodes

    @staticmethod
    def _extract_unpublished(item: Dict[str, Any], key: str) -> List[str]:
        host = "unknown"
        pub_on = ""
        return HostingNode(
            host=host, published_on=CMIP6ItemRecord._parse_datetime(pub_on)
        )

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
    def has_parts(self) -> List[str]:
        return self._has_parts

    @property
    def is_part_of(self) -> str:
        return self._is_part_of

    @property
    def hosting_node(self) -> HostingNode:
        return self._hosting_node

    @property
    def replica_nodes(self) -> List[HostingNode]:
        return self._replica_nodes

    @property
    def unpublished_replicas(self) -> List[str]:
        return self._unpublished_replicas

    @property
    def unpublished_hosts(self) -> List[str]:
        return self._unpublished_hosts

    def as_handle_model(self) -> CMIP6ItemModel:
        return CMIP6ItemModel(
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

    def as_record(self) -> dict:
        """Return the handle model as dict."""
        return self.as_handle_model().model_dump()

    def as_json(self) -> str:
        """Return the handle model as JSON string."""
        return self.as_handle_model().model_dump_json()
