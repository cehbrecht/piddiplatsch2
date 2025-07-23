import pluggy
import logging
import uuid
from typing import Any, Dict
from jsonschema import validate, ValidationError
from piddiplatsch.schema import CMIP6_SCHEMA as SCHEMA
from piddiplatsch.records import CMIP6ItemRecord
from piddiplatsch.records.utils import extract_asset_records

hookimpl = pluggy.HookimplMarker("piddiplatsch")


class CMIP6Processor:
    """Pluggy processor for CMIP6 STAC items."""

    @hookimpl
    def process(self, key: str, value: Dict[str, Any], handle_client: Any) -> None:
        """Process a Kafka message for a CMIP6 STAC item and register it in the Handle Service."""
        logging.debug("CMIP6 plugin processing key: %s", key)

        try:
            item = value["data"]["payload"]["item"]
        except KeyError as e:
            logging.error("Missing 'item' in Kafka message: %s", e)
            raise ValueError("Missing 'item' in Kafka message") from e

        try:
            validate(instance=item, schema=SCHEMA)
        except ValidationError as e:
            logging.error(
                "Schema validation failed at %s: %s", list(e.absolute_path), e.message
            )
            raise ValueError(f"Invalid CMIP6 STAC item: {e.message}") from e

        record = CMIP6ItemRecord(item, strict=False)

        logging.debug(
            "Register item record for PID %s: %s", record.pid, record.as_record()
        )
        handle_client.add_item(record.pid, record.as_record())

        # Iterate over file assets and register them as well
        asset_records = extract_asset_records(
            item, exclude_keys=["reference_file", "thumbnail", "quicklook"]
        )

        for record in asset_records:
            logging.debug(
                "Register assert record for PID %s: %s", record.pid, record.as_record()
            )
            handle_client.add_item(record.pid, record.as_record())
