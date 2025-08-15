import time
from typing import Any

import jsonpatch
from jsonschema import validate
from pluggy import HookimplMarker

from piddiplatsch.config import config
from piddiplatsch.processing import BaseProcessor, ProcessingResult
from piddiplatsch.records import CMIP6DatasetRecord
from piddiplatsch.records.cmip6_file_record import extract_asset_records
from piddiplatsch.schema import CMIP6_SCHEMA as SCHEMA
from piddiplatsch.utils.stac import get_stac_client

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
        self.stac_client = get_stac_client()

    @hookimpl
    def process(self, key: str, value: dict[str, Any]):
        self.logger.debug(f"CMIP6 plugin processing key={key}")
        start_total = time.perf_counter()

        try:
            num_handles, schema_time, record_time, handle_time, skipped = (
                self._do_process(value, key)
            )
        except ValueError:
            raise

        elapsed_total = time.perf_counter() - start_total

        return ProcessingResult(
            key=key,
            num_handles=num_handles,
            success=True,
            elapsed=elapsed_total,
            schema_validation_time=schema_time,
            record_validation_time=record_time,
            handle_processing_time=handle_time,
            skipped=skipped,
        )

    def _do_process(self, value, key):
        payload = value.get("data", {}).get("payload", {})
        if not payload:
            self.logger.warning(f"[MISSING payload skipped] key={key}")
            return 0, 0.0, 0.0, 0.0, True

        return self._process_payload(payload, value.get("metadata", {}), key)

    def _process_payload(
        self, payload: dict[str, Any], metadata: dict[str, Any], key: str
    ):
        """Decide how to process the payload: PATCH or full item."""
        skipped = False

        if payload.get("method") == "PATCH":
            try:
                item = self._apply_patch_to_stac_item(payload)
            except Exception as e:
                self.logger.error(f"Failed to apply patch for key={key}: {e}")
                return 0, 0.0, 0.0, 0.0, True

        elif "item" in payload:
            item = payload["item"]
        else:
            self.logger.warning(f"[MISSING item skipped] key={key}")
            skipped = True
            return 0, 0.0, 0.0, 0.0, skipped

        # schema validation
        _, schema_time = self._time_function(validate, instance=item, schema=SCHEMA)

        # model validation
        additional_attrs = {"publication_time": metadata.get("time")}
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

        return num_handles, schema_time, record_time, handle_time, skipped

    def _apply_patch_to_stac_item(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Fetch the STAC item and apply a JSON patch."""
        collection_id = payload["collection_id"]
        item_id = payload["item_id"]
        patch_data = payload["patch"]

        item = self.stac_client.get_item(collection_id, item_id)
        patch_obj = jsonpatch.JsonPatch(patch_data["operations"])
        patched_item = patch_obj.apply(item)

        self.logger.debug(f"Applied patch to STAC item {collection_id}/{item_id}")
        return patched_item
