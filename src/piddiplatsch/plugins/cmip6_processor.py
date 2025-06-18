import pluggy
import logging
import uuid
from typing import Any, Dict
from jsonschema import validate, ValidationError
from piddiplatsch.schema import CMIP6_SCHEMA as SCHEMA

hookimpl = pluggy.HookimplMarker("piddiplatsch")

logger = logging.getLogger(__name__)


class CMIP6Processor:
    """Pluggy processor for CMIP6 STAC items."""

    @hookimpl
    def process(self, key: str, value: Dict[str, Any], handle_client: Any) -> None:
        """Process a Kafka message for a CMIP6 STAC item and register it in the Handle Service."""
        logger.info("CMIP6 plugin processing key: %s", key)

        try:
            item = value["data"]["payload"]["item"]
        except KeyError as e:
            logger.error("Missing 'item' in Kafka message: %s", e)
            raise ValueError("Missing 'item' in Kafka message") from e

        try:
            validate(instance=item, schema=SCHEMA)
        except ValidationError as e:
            logger.error(
                "Schema validation failed at %s: %s", list(e.absolute_path), e.message
            )
            raise ValueError(f"Invalid CMIP6 STAC item: {e.message}") from e

        try:
            id = item.get("id")
            pid = uuid.uuid3(uuid.NAMESPACE_URL, id)
            url = item["links"][0]["href"]
            parts = id.rsplit(".", 1)
            dataset_id = parts[0]
            version = parts[1] if len(parts) > 1 else None
            ref_node = item["assets"].get("reference_file", {}).get("alternate:name")
            data_node = item["assets"].get("data0001", {}).get("alternate:name")
            hosting_node = ref_node or data_node or "unknown"
        except (IndexError, KeyError) as e:
            logger.error("Error extracting fields from item: %s", e)
            raise ValueError("Missing required fields in item") from e

        record = {
            "PID": pid,
            "URL": url,  # might not be used
            "AGGREGATION_LEVEL": "Dataset",
            "DATASET_ID": dataset_id,  # id without version
            "DATASET_VERSION": version,  # version without v. YYYYMMDD
            "HOSTING_NODE": hosting_node,  # should be data_node? list ... published_on ...
            "REPLICA_NODE": "",
            "UNPUBLISHED_REPLICAS": "",
            "UNPUBLISHED_HOSTS": "",
        }

        logger.debug("Generated record for PID %s: %s", pid, record)
        handle_client.add_item(pid, record)
