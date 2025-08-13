import logging
from collections.abc import Sequence
from datetime import datetime
from typing import Any

import pluggy
from jsonschema import ValidationError, validate

from piddiplatsch.config import config  # loads TOML config
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

        # Load excluded keys from config, with fallback to defaults
        default_excluded = ["reference_file", "globus", "thumbnail", "quicklook"]
        self.excluded_asset_keys = excluded_asset_keys or config.get("cmip6", {}).get(
            "excluded_asset_keys", default_excluded
        )

    @hookimpl
    def process(self, key: str, value: dict[str, Any]) -> ProcessingResult:
        logging.debug(f"CMIP6 plugin processing key={key}")
        start = datetime.now()

        try:
            num_handles = self._do_process(value, key)
            elapsed = (datetime.now() - start).total_seconds()
            return ProcessingResult(
                key=key,
                num_handles=num_handles,
                elapsed=elapsed,
                success=True,
            )
        except Exception as e:
            logging.exception(f"Processing of key={key} failed: {e}")
            return ProcessingResult(
                key=key,
                success=False,
                error=str(e),
            )

    def _do_process(self, value: dict[str, Any], key: str) -> int:
        payload = value.get("data", {}).get("payload", {})

        if payload.get("method") == "PATCH":
            return self._process_patch_message(payload, key)
        else:
            return self._process_item_message(payload, value, key)

    def _process_patch_message(self, payload: dict[str, Any], key: str) -> int:
        logging.warning(f"[PATCH skipped] item_id={payload.get('item_id')} key={key}")
        return 0  # Instead of raising, skip gracefully

    def _process_item_message(
        self, payload: dict[str, Any], value: dict[str, Any], key: str
    ) -> int:
        num_handles = 0

        try:
            item = payload["item"]
        except KeyError as e:
            logging.error(f"Missing 'item' in Kafka message key={key}: {e}")
            raise ValueError("Missing 'item' in Kafka message") from e

        self._validate_item(item, key)

        additional_attrs = self._get_additional_attributes(value)

        # Dataset record
        dataset_record = CMIP6DatasetRecord(
            item,
            strict=self.strict,
            exclude_keys=self.excluded_asset_keys,
            additional_attributes=additional_attrs,
        )
        dataset_record.validate()

        logging.debug(f"Registering dataset PID {dataset_record.pid} (key={key})")
        self._safe_add_record(dataset_record)

        num_handles += 1

        # Asset records
        asset_records = extract_asset_records(
            item, exclude_keys=self.excluded_asset_keys, strict=self.strict
        )
        if not asset_records:
            logging.warning(
                f"No file assets found for dataset PID {dataset_record.pid}"
            )
        else:
            logging.debug(f"Found {len(asset_records)} asset records to register")

        for asset_record in asset_records:
            logging.debug(f"Registering asset PID {asset_record.pid} (key={key})")
            self._safe_add_record(asset_record)
            num_handles += 1

        return num_handles

    def _safe_add_record(self, record) -> None:
        """Adds a record with optional retry logic."""
        last_err = None
        for attempt in range(self.retries + 1):
            try:
                self.handle_client.add_record(record.pid, record.as_record())
                return
            except Exception as e:
                last_err = e
                logging.warning(
                    f"Attempt {attempt + 1} failed to add PID {record.pid}: {e}"
                )
        # If we reach here, all retries failed
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
        return {
            "publication_time": publication_time,
        }
