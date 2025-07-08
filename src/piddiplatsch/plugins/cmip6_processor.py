import pluggy
import logging
import uuid
from typing import Any, Dict
from jsonschema import validate, ValidationError
from piddiplatsch.schema import CMIP6_SCHEMA as SCHEMA
from piddiplatsch.records import CMIP6DatasetRecord

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

        record = CMIP6DatasetRecord(item, strict=False)

        logger.debug("Generated record for PID %s: %s", record.pid, record.as_record())
        handle_client.add_item(record.pid, record.as_record())
