import time
from typing import Any

from jsonschema import validate
from pluggy import HookimplMarker

from piddiplatsch.config import config
from piddiplatsch.processing import BaseProcessor, ProcessingResult
from piddiplatsch.records import CMIP6DatasetRecord
from piddiplatsch.records.cmip6_file_record import extract_asset_records
from piddiplatsch.schema import CMIP6_SCHEMA as SCHEMA

hookimpl = HookimplMarker("piddiplatsch")


class CMIP6Processor(BaseProcessor):
    """CMIP6-specific processor logic."""

    EXCLUDED_ASSET_KEYS = ["reference_file", "globus", "thumbnail", "quicklook"]

    def __init__(self, strict=False, excluded_asset_keys=None, **kwargs):
        super().__init__(**kwargs)
        self.strict = strict
        self.excluded_asset_keys = excluded_asset_keys or config.get("cmip6", {}).get(
            "excluded_asset_keys", self.EXCLUDED_ASSET_KEYS
        )

    @hookimpl
    def process(self, key: str, value: dict[str, Any]):
        self.logger.debug(f"CMIP6 plugin processing key={key}")
        start_total = time.perf_counter()

        try:
            num_handles, schema_time, record_time, handle_time = (
                self._process_item_message(value, key)
            )
            elapsed_total = time.perf_counter() - start_total

            return ProcessingResult(
                key=key,
                num_handles=num_handles,
                success=True,
                elapsed=elapsed_total,
                schema_validation_time=schema_time,
                record_validation_time=record_time,
                handle_processing_time=handle_time,
            )
        except Exception as e:
            self.logger.exception(f"Processing of key={key} failed")
            return ProcessingResult(key=key, success=False, error=str(e))

    def _process_item_message(self, value, key):
        payload = value.get("data", {}).get("payload", {})
        if payload.get("method") == "PATCH":
            self.logger.warning(
                f"[PATCH skipped] item_id={payload.get('item_id')} key={key}"
            )
            return 0, 0.0, 0.0, 0.0

        if "item" not in payload:
            raise ValueError("Missing 'item' in Kafka message payload")

        item = payload["item"]

        # schema validation
        _, schema_time = self._time_function(validate, instance=item, schema=SCHEMA)

        # model validation
        additional_attrs = {"publication_time": value.get("metadata", {}).get("time")}
        record = CMIP6DatasetRecord(
            item,
            strict=self.strict,
            exclude_keys=self.excluded_asset_keys,
            additional_attributes=additional_attrs,
        )
        _, record_time = self._time_function(record.validate)

        # handle processing
        def add_records():
            self._safe_add_record(record)
            num_handles = 1
            for r in extract_asset_records(
                item, exclude_keys=self.excluded_asset_keys, strict=self.strict
            ):
                self._safe_add_record(r)
                num_handles += 1
            return num_handles

        num_handles, handle_time = self._time_function(add_records)

        return num_handles, schema_time, record_time, handle_time
