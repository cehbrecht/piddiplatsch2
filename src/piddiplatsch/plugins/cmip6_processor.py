import logging
from datetime import datetime
from typing import Any

import pluggy
from jsonschema import ValidationError, validate

from piddiplatsch.handle_client import HandleClient
from piddiplatsch.records import CMIP6DatasetRecord
from piddiplatsch.records.cmip6_file_record import extract_asset_records
from piddiplatsch.result import ProcessingResult
from piddiplatsch.schema import CMIP6_SCHEMA as SCHEMA

hookimpl = pluggy.HookimplMarker("piddiplatsch")


class CMIP6Processor:
    """Pluggy processor for CMIP6 STAC items."""

    EXCLUDED_ASSET_KEYS = ["reference_file", "globus", "thumbnail", "quicklook"]

    def __init__(self, strict: bool = False):
        self.strict = strict
        self.handle_client = HandleClient.from_config()

    @hookimpl
    def process(self, key: str, value: dict[str, Any]) -> ProcessingResult:
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
            logging.exception(f"Processing of {key} failed with error: {e}")
            return ProcessingResult(
                key=key,
                success=False,
                error=str(e),
            )

    def _do_process(self, value: dict[str, Any]) -> int:
        payload = value.get("data", {}).get("payload", {})

        if payload.get("method") == "PATCH":
            return self._process_patch_message(payload)
        else:
            return self._process_item_message(payload, value)

    def _process_patch_message(self, payload: dict[str, Any]) -> int:
        logging.warning(f"PATCH message detected for item_id={payload.get('item_id')}")
        raise NotImplementedError("PATCH messages are not implemented yet")

    def _process_item_message(
        self, payload: dict[str, Any], value: dict[str, Any]
    ) -> int:
        num_handles = 0
        try:
            item = payload["item"]
        except KeyError as e:
            logging.error(f"Missing 'item' in Kafka message: {e}")
            raise ValueError("Missing 'item' in Kafka message") from e

        self._validate_item(item)

        additional_attrs = self._get_additional_attributes(value)

        record = CMIP6DatasetRecord(
            item,
            strict=self.strict,
            exclude_keys=self.EXCLUDED_ASSET_KEYS,
            additional_attributes=additional_attrs,
        )
        record.validate()

        logging.debug(f"Register item record for PID {record.pid}")
        self.handle_client.add_record(record.pid, record.as_record())
        num_handles += 1

        asset_records = extract_asset_records(
            item, exclude_keys=self.EXCLUDED_ASSET_KEYS, strict=self.strict
        )
        if not asset_records:
            logging.warning(f"No file assets found for item PID {record.pid}")
        else:
            logging.debug(f"Found {len(asset_records)} asset records to register")

        for record in asset_records:
            logging.debug(
                f"Register asset record for PID {record.pid}: {record.as_record()}"
            )
            self.handle_client.add_record(record.pid, record.as_record())
            num_handles += 1

        return num_handles

    def _validate_item(self, item: dict[str, Any]) -> None:
        try:
            validate(instance=item, schema=SCHEMA)
        except ValidationError as e:
            logging.error(
                f"Schema validation failed at {list(e.absolute_path)}: {e.message}"
            )
            raise ValueError(f"Invalid CMIP6 STAC item: {e.message}") from e

    def _get_additional_attributes(self, value: dict[str, Any]) -> dict[str, Any]:
        publication_time = value.get("metadata", {}).get("time")
        return {
            "publication_time": publication_time,
        }
