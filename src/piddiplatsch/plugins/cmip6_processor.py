import time
from typing import Any

import jsonpatch
from pydantic import ValidationError

from piddiplatsch.config import config
from piddiplatsch.processing import BaseProcessor, ProcessingResult
from piddiplatsch.records import CMIP6DatasetRecord
from piddiplatsch.records.cmip6_file_record import extract_asset_records
from piddiplatsch.utils.stac import get_stac_client


class CMIP6Processor(BaseProcessor):
    """CMIP6-specific processor logic."""

    EXCLUDED_ASSET_KEYS = ["reference_file", "globus", "thumbnail", "quicklook"]

    def __init__(self, excluded_asset_keys=None, **kwargs):
        super().__init__(**kwargs)
        self.excluded_asset_keys = excluded_asset_keys or config.get("cmip6", {}).get(
            "excluded_asset_keys", self.EXCLUDED_ASSET_KEYS
        )
        self.stac_client = get_stac_client()

    def process(self, key: str, value: dict[str, Any]) -> ProcessingResult:
        self.logger.debug(f"CMIP6 plugin processing key={key}")
        start_total = time.perf_counter()

        result = ProcessingResult(key=key)

        try:
            result = self._do_process(value, key, result)
        except ValidationError as e:
            self.logger.error(f"Validation error for key={key}: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Processing error for key={key}: {e}")
            raise

        result.elapsed = time.perf_counter() - start_total
        result.success = True

        return result

    def _do_process(
        self, value: dict[str, Any], key: str, result: ProcessingResult
    ) -> tuple[int, float, float, float, bool]:
        """Check payload presence and delegate to payload processor."""
        payload = value.get("data", {}).get("payload")
        if not payload:
            self._log_skipped(key, "MISSING payload")
            result.skipped = True
            return result

        metadata = value.get("metadata", {})
        return self._process_payload(payload, metadata, key, result)

    def _process_payload(
        self,
        payload: dict[str, Any],
        metadata: dict[str, Any],
        key: str,
        result: ProcessingResult,
    ) -> tuple[int, float, float, float, bool]:
        """Decide how to process the payload: PATCH or full item."""
        if payload.get("method") == "PATCH":
            try:
                item = self._apply_patch_to_stac_item(payload)
                self.logger.info(f"Patched item with key={key}.")
                result.patched = True
            except Exception as e:
                self.logger.error(f"Failed to apply patch for key={key}: {e}")
                result.skipped = True
                return result
        elif "item" in payload:
            item = payload["item"]
        else:
            self._log_skipped(key, "MISSING item")
            result.skipped = True
            return result

        # Create record
        additional_attrs = {"publication_time": metadata.get("time")}
        record = CMIP6DatasetRecord(
            item,
            exclude_keys=self.excluded_asset_keys,
            additional_attributes=additional_attrs,
        )
        # Validate record
        try:
            record.validate()
        except Exception as e:
            self.logger.error(f"Validation failed for key={key}: {e}")
            raise

        # Handle processing
        num_handles, handle_time = self._add_records_from_item(record, item)
        result.num_handles = num_handles
        result.handle_processing_time = handle_time

        return result

    def _apply_patch_to_stac_item(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Fetch the STAC item and apply a JSON patch."""
        collection_id = payload["collection_id"]
        item_id = payload["item_id"]
        patch_data = payload["patch"]

        item = self.stac_client.get_item(collection_id, item_id)
        if item is None:
            raise ValueError(f"STAC item {collection_id}/{item_id} not found")

        patch_obj = jsonpatch.JsonPatch(patch_data["operations"])
        patched_item = patch_obj.apply(item)

        self.logger.info(f"Applied patch to STAC item {collection_id}/{item_id}")
        self.logger.debug(f"patched item: {patched_item}")
        return patched_item

    def _add_records_from_item(
        self, record: CMIP6DatasetRecord, item: dict[str, Any]
    ) -> tuple[int, float]:
        """Add main record and extracted asset records, measuring handle processing time."""

        def add_records():
            self._safe_add_record(record)
            num_handles = 1
            for r in extract_asset_records(item, exclude_keys=self.excluded_asset_keys):
                self._safe_add_record(r)
                num_handles += 1
            return num_handles

        num_handles, handle_time = self._time_function(add_records)
        return num_handles, handle_time

    def _log_skipped(self, key: str, reason: str):
        """Standardized logging for skipped messages."""
        self.logger.warning(f"[{reason} skipped] key={key}")
