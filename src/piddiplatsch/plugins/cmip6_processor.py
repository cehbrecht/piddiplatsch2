import pluggy
import logging
import uuid
from datetime import datetime
from typing import Any, Dict
from jsonschema import validate, ValidationError
from piddiplatsch.schema import CMIP6_SCHEMA as SCHEMA
from piddiplatsch.records import CMIP6ItemRecord
from piddiplatsch.records.utils import extract_asset_records
from piddiplatsch.result import ProcessingResult
from piddiplatsch.handle_client import HandleClient

hookimpl = pluggy.HookimplMarker("piddiplatsch")


class CMIP6Processor:
    """Pluggy processor for CMIP6 STAC items."""

    def __init__(self):
        self.handle_client = HandleClient.from_config()

    @hookimpl
    def process(self, key: str, value: Dict[str, Any]) -> ProcessingResult:
        """Process a Kafka message for a CMIP6 STAC item and register it in the Handle Service."""
        logging.debug(f"CMIP6 plugin processing key: {key}")

        start = datetime.now()

        try:
            num_handles = self._do_process(value)
            elapsed = (datetime.now() - start).total_seconds()
            return ProcessingResult(
                key=key,
                num_handles=num_handles,
                elapsed=elapsed,
                success=True,
            )
        except Exception as e:
            return ProcessingResult(key=key, success=False, error=str(e))

    def _do_process(self, value: Dict[str, Any]) -> int:
        num_handles = 0

        try:
            item = value["data"]["payload"]["item"]
        except KeyError as e:
            logging.error(f"Missing 'item' in Kafka message: {e}")
            raise ValueError("Missing 'item' in Kafka message") from e

        try:
            validate(instance=item, schema=SCHEMA)
        except ValidationError as e:
            logging.error(
                f"Schema validation failed at {list(e.absolute_path)}: {e.message}"
            )
            raise ValueError(f"Invalid CMIP6 STAC item: {e.message}") from e

        record = CMIP6ItemRecord(item, strict=False)

        logging.debug(
            f"Register item record for PID {record.pid}: {record.as_record()}"
        )
        self.handle_client.add_item(record.pid, record.as_record())
        num_handles += 1

        # Iterate over file assets and register them as well
        asset_records = extract_asset_records(
            item, exclude_keys=["reference_file", "thumbnail", "quicklook"]
        )

        for record in asset_records:
            logging.debug(
                f"Register assert record for PID {record.pid}: {record.as_record()}"
            )
            self.handle_client.add_item(record.pid, record.as_record())
            num_handles += 1

        return num_handles
