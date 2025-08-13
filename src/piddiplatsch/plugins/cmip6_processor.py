import logging
import time
from collections.abc import Sequence
from typing import Any

import pluggy
from jsonschema import ValidationError, validate

from piddiplatsch.config import config
from piddiplatsch.handle_client import HandleClient
from piddiplatsch.processing.result import ProcessingResult
from piddiplatsch.records import CMIP6DatasetRecord
from piddiplatsch.records.cmip6_file_record import extract_asset_records
from piddiplatsch.schema import CMIP6_SCHEMA as SCHEMA

hookimpl = pluggy.HookimplMarker("piddiplatsch")


class CMIP6Processor:
    """Pluggy processor for CMIP6 STAC items."""

    def __init__(
        self,
        strict: bool = False,
        handle_client: HandleClient | None = None,
        excluded_asset_keys: Sequence[str] | None = None,
        retries: int = 0,
    ):
        self.strict = strict
        self.handle_client = handle_client or HandleClient.from_config()
        self.retries = retries

        default_excluded = ["reference_file", "globus", "thumbnail", "quicklook"]
        self.excluded_asset_keys = excluded_asset_keys or config.get("cmip6", {}).get(
            "excluded_asset_keys", default_excluded
        )

    @hookimpl
    def process(self, key: str, value: dict[str, Any]) -> ProcessingResult:
        logging.debug(f"CMIP6 plugin processing key={key}")
        start_total = time.perf_counter()

        try:
            num_handles, schema_time, record_time, handle_time = self._do_process(
                value, key
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
            logging.exception(f"Processing of key={key} failed")
            return ProcessingResult(key=key, success=False, error=str(e))

    def _do_process(
        self, value: dict[str, Any], key: str
    ) -> tuple[int, float, float, float]:
        payload = value.get("data", {}).get("payload", {})
        if payload.get("method") == "PATCH":
            logging.warning(
                f"[PATCH skipped] item_id={payload.get('item_id')} key={key}"
            )
            return 0, 0.0, 0.0, 0.0

        return self._process_item_message(payload, value, key)

    def _process_item_message(
        self, payload: dict[str, Any], value: dict[str, Any], key: str
    ) -> tuple[int, float, float, float]:
        if "item" not in payload:
            raise ValueError("Missing 'item' in Kafka message payload")

        item = payload["item"]

        # 1️⃣ Schema validation timing
        t0 = time.perf_counter()
        self._validate_item(item, key)
        schema_time = time.perf_counter() - t0

        # 2️⃣ Model validation timing
        additional_attrs = self._get_additional_attributes(value)
        record = CMIP6DatasetRecord(
            item,
            strict=self.strict,
            exclude_keys=self.excluded_asset_keys,
            additional_attributes=additional_attrs,
        )
        t1 = time.perf_counter()
        record.validate()
        record_time = time.perf_counter() - t1

        # 3️⃣ Handle processing timing
        t2 = time.perf_counter()
        self._safe_add_record(record)
        num_handles = 1

        asset_records = extract_asset_records(
            item, exclude_keys=self.excluded_asset_keys, strict=self.strict
        )
        for r in asset_records:
            self._safe_add_record(r)
            num_handles += 1
        handle_time = time.perf_counter() - t2

        return num_handles, schema_time, record_time, handle_time

    def _safe_add_record(self, record) -> None:
        last_err = None
        for attempt in range(1, self.retries + 2):  # +1 for initial try
            try:
                self.handle_client.add_record(record.pid, record.as_record())
                return
            except Exception as e:
                last_err = e
                logging.warning(
                    f"Attempt {attempt} failed to add PID {record.pid}: {e}"
                )
        raise RuntimeError(
            f"Failed to register handle {record.pid} after {self.retries + 1} attempts"
        ) from last_err

    def _validate_item(self, item: dict[str, Any], key: str) -> None:
        try:
            validate(instance=item, schema=SCHEMA)
        except ValidationError as e:
            logging.error(
                f"Schema validation failed for key={key} "
                f"at {list(e.absolute_path)}: {e.message}"
            )
            raise ValueError(f"Invalid CMIP6 STAC item: {e.message}") from e

    def _get_additional_attributes(self, value: dict[str, Any]) -> dict[str, Any]:
        publication_time = value.get("metadata", {}).get("time")
        return {"publication_time": publication_time}
