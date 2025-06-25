import logging
from typing import Any, Dict
from uuid import uuid3, NAMESPACE_URL

from jsonschema import validate, ValidationError

from piddiplatsch.schema import CMIP6_SCHEMA as SCHEMA
from piddiplatsch.models import CMIP6HandleModel, HostingNode

logger = logging.getLogger(__name__)


class CMIP6Record:
    """Wraps a validated CMIP6 STAC item and prepares Handle records."""

    def __init__(self, item: Dict[str, Any]):
        self.item = item

        # Validate the STAC item against schema
        try:
            validate(instance=self.item, schema=SCHEMA)
        except ValidationError as e:
            logger.error(
                "Schema validation failed at %s: %s", list(e.absolute_path), e.message
            )
            raise ValueError(f"Invalid CMIP6 STAC item: {e.message}") from e

        # Extract fields
        self._pid = None
        self._url = None
        self._dataset_id = None
        self._dataset_version = None
        self._hosting_node = None
        self._replica_nodes = []
        self._unpublished_replicas = []
        self._unpublished_hosts = []

        self._extract_fields()

    def _extract_fields(self):
        # ID and PID
        try:
            id_str = self.item["id"]
        except KeyError as e:
            logger.error("Missing 'id' in item: %s", e)
            raise ValueError("Missing required 'id' field") from e

        self._pid = uuid3(NAMESPACE_URL, id_str)

        # URL from first link href
        try:
            self._url = self.item["links"][0]["href"]
        except (KeyError, IndexError) as e:
            logger.error("Missing 'links[0].href' in item: %s", e)
            raise ValueError("Missing required 'links[0].href' field") from e

        # Dataset ID and version
        parts = id_str.rsplit(".", 1)
        self._dataset_id = parts[0]
        self._dataset_version = parts[1] if len(parts) > 1 else None

        # Hosting node from assets
        ref_node = self.item.get("assets", {}).get("reference_file", {}).get("alternate:name")
        data_node = self.item.get("assets", {}).get("data0001", {}).get("alternate:name")
        host = ref_node or data_node or "unknown"

        # published_on for hosting_node - from custom field published_on in assets? or external XML metadata?
        # For now, let's try published_on from item["assets"][key]["published_on"], fallback None
        pub_on = None
        for key in ["reference_file", "data0001"]:
            published = self.item.get("assets", {}).get(key, {}).get("published_on")
            if published:
                pub_on = published
                break

        self._hosting_node = HostingNode(host=host, published_on=self._parse_datetime(pub_on))

        # Replica nodes from item["locations"]["location"], if present
        self._replica_nodes = []
        locations = self.item.get("locations")
        if locations:
            locs = locations.get("location", [])
            if isinstance(locs, dict):
                # single location dict -> wrap in list
                locs = [locs]
            for loc in locs:
                h = loc.get("host")
                p = self._parse_datetime(loc.get("publishedOn"))
                if h:
                    self._replica_nodes.append(HostingNode(host=h, published_on=p))

        # Unpublished replicas and hosts, optional fields (empty lists if none)
        self._unpublished_replicas = self.item.get("unpublished_replicas", [])
        self._unpublished_hosts = self.item.get("unpublished_hosts", [])

    @staticmethod
    def _parse_datetime(value) -> Any:
        if not value:
            return None
        # ISO 8601 parse
        from dateutil.parser import isoparse

        try:
            return isoparse(value)
        except Exception:
            logger.warning(f"Failed to parse datetime: {value}")
            return None

    @property
    def pid(self):
        return self._pid

    @property
    def url(self):
        return self._url

    @property
    def hosting_node(self):
        return self._hosting_node

    @property
    def replica_nodes(self):
        return self._replica_nodes

    def as_handle_model(self) -> CMIP6HandleModel:
        return CMIP6HandleModel(
            PID=self.pid,
            URL=self.url,
            DATASET_ID=self._dataset_id,
            DATASET_VERSION=self._dataset_version,
            HOSTING_NODE=self.hosting_node,
            REPLICA_NODES=self.replica_nodes,
            UNPUBLISHED_REPLICAS=self._unpublished_replicas,
            UNPUBLISHED_HOSTS=self._unpublished_hosts,
        )
