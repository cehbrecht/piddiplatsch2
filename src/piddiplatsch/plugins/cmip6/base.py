from __future__ import annotations

import uuid
from functools import cached_property
from typing import Any

from pydantic import BaseModel, HttpUrl, PrivateAttr, field_serializer

from piddiplatsch.config import config
from piddiplatsch.helpers import utc_now
from piddiplatsch.records.base import BaseRecord
from piddiplatsch.utils.models import drop_empty


class BaseCMIP6Model(BaseModel):
    _PID: str | None = PrivateAttr(default=None)

    ESGF: str = "ESGF2 TEST"
    URL: HttpUrl

    @field_serializer("URL")
    def _serialize_url(self, v: HttpUrl) -> str:
        return str(v)

    def set_pid(self, value: str | uuid.UUID) -> None:
        if isinstance(value, uuid.UUID):
            self._PID = str(value)
        elif isinstance(value, str):
            try:
                self._PID = str(uuid.UUID(value))
            except ValueError:
                raise ValueError(f"Invalid PID string: {value} is not a valid UUID.")
        else:
            raise TypeError(
                f"PID must be a UUID or UUID string, got {type(value).__name__}"
            )

    def get_pid(self) -> str | None:
        return self._PID


class BaseCMIP6Record(BaseRecord):
    def __init__(
        self,
        item: dict[str, Any],
        additional_attributes: dict[str, Any] | None = None,
    ):
        super().__init__(item)
        self.additional_attributes = additional_attributes or {}

    @cached_property
    def prefix(self) -> str:
        return config.get("handle", {}).get("prefix", "")

    @cached_property
    def landing_page_url(self) -> str:
        return config.get("cmip6", {}).get("landing_page_url", "").rstrip("/")

    @cached_property
    def default_publication_time(self) -> str:
        published_on = self.additional_attributes.get("publication_time")
        if not published_on:
            published_on = utc_now().strftime("%Y-%m-%d %H:%M:%S")
        return published_on

    @cached_property
    def item_id(self) -> str:
        if "id" not in self.item:
            raise KeyError("Missing 'id' in item")
        return self.item["id"]

    @cached_property
    def assets(self) -> dict[str, Any]:
        return self.item.get("assets", {})

    def get_asset(self, key: str) -> dict[str, Any]:
        return self.assets.get(key, {})

    def get_asset_property(self, key: str, prop: str, default: Any = None) -> Any:
        return self.get_asset(key).get(prop, default)

    @cached_property
    def properties(self) -> dict[str, Any]:
        return self.item.get("properties", {})

    def get_property(self, key: str, default: Any = None) -> Any:
        return self.properties.get(key, default)

    def validate(self):
        try:
            _ = self.model
        except Exception as e:
            raise ValueError(f"Pydantic validation failed: {e}") from e

    def as_record(self) -> dict:
        return drop_empty(self.model.model_dump())

    def as_json(self) -> str:
        return self.model.model_dump_json()

    @cached_property
    def url(self) -> str:
        return f"{self.landing_page_url}/{self.prefix}/{self.pid}"
