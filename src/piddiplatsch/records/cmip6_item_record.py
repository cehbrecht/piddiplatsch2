import logging
from typing import Any, Dict, List, Optional
from uuid import uuid3, NAMESPACE_URL
from datetime import datetime

from jsonschema import validate, ValidationError
from dateutil.parser import isoparse

from piddiplatsch.schema import CMIP6_SCHEMA as SCHEMA
from piddiplatsch.models import CMIP6ItemModel, HostingNode

logger = logging.getLogger(__name__)


class CMIP6ItemRecord:
    """Wraps a validated CMIP6 STAC item and prepares Handle records."""

    def __init__(self, item: Dict[str, Any], strict: bool):
        self.item = item
        self.strict = strict

        # Validate the STAC item against schema
        try:
            validate(instance=self.item, schema=SCHEMA)
        except ValidationError as e:
            logger.error(
                "Schema validation failed at %s: %s", list(e.absolute_path), e.message
            )
            raise ValueError(f"Invalid CMIP6 STAC item: {e.message}") from e

        self._pid = self._extract_pid(self.item)
        self._url = self._extract_url(self.item)
        self._dataset_id, self._dataset_version = self._extract_dataset_version(
            self.item
        )
        self._hosting_node = self._extract_hosting_node(self.item)
        self._replica_nodes = self._extract_replica_nodes(self.item)
        self._unpublished_replicas = self._extract_unpublished(
            self.item, "unpublished_replicas"
        )
        self._unpublished_hosts = self._extract_unpublished(
            self.item, "unpublished_hosts"
        )

    @staticmethod
    def _extract_pid(item: Dict[str, Any]) -> Any:
        try:
            id_str = item["id"]
        except KeyError as e:
            logger.error("Missing 'id' in item: %s", e)
            raise ValueError("Missing required 'id' field") from e

        return uuid3(NAMESPACE_URL, id_str)

    @staticmethod
    def _extract_url(item: Dict[str, Any]) -> str:
        try:
            return item["links"][0]["href"]
        except (KeyError, IndexError) as e:
            logger.error("Missing 'links[0].href' in item: %s", e)
            raise ValueError("Missing required 'links[0].href' field") from e

    @staticmethod
    def _extract_dataset_version(item: Dict[str, Any]) -> (str, Optional[str]):
        id_str = item.get("id", "")
        parts = id_str.rsplit(".", 1)
        dataset_id = parts[0]
        dataset_version = parts[1] if len(parts) > 1 else None
        return dataset_id, dataset_version

    @staticmethod
    def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            return isoparse(value)
        except Exception:
            logger.warning(f"Failed to parse datetime: {value}")
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
        val = item.get(key)
        if val is None:
            return []
        if not isinstance(val, list):
            logger.warning(
                f"Expected list for '{key}', got {type(val)}. Coercing to list."
            )
            return [str(val)]
        return val

    @property
    def pid(self) -> Any:
        return self._pid

    @property
    def url(self) -> str:
        return self._url

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
            PID=self.pid,
            URL=self.url,
            DATASET_ID=self._dataset_id,
            DATASET_VERSION=self._dataset_version,
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
