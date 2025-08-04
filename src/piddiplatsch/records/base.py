from abc import ABC, abstractmethod
from functools import cached_property
from typing import Any

from pydantic import BaseModel, ValidationError

from piddiplatsch.config import config
from piddiplatsch.records.utils import drop_empty


class BaseRecord(ABC):
    def __init__(self, item: dict[str, Any], strict: bool):
        self.item = item
        self.strict = strict

    @abstractmethod
    def as_handle_model(self) -> BaseModel:
        """Return a Pydantic model representing the handle record."""
        ...

    @cached_property
    def model(self) -> BaseModel:
        return self.as_handle_model()

    def validate(self):
        try:
            _ = self.model
        except ValidationError as e:
            raise ValueError(f"Pydantic validation failed: {e}") from e

    def as_record(self) -> dict:
        return drop_empty(self.model.model_dump())

    def as_json(self) -> str:
        return self.model.model_dump_json()

    @property
    def is_strict(self) -> bool:
        return self.strict

    def __repr__(self) -> str:
        pid = getattr(self, "pid", lambda: "UNKNOWN")
        return f"<{self.__class__.__name__} id={self.item.get('id', 'UNKNOWN')} pid={pid()}>"

    def __str__(self):
        return self.__repr__()


class BaseCMIP6Record(BaseRecord):
    def __init__(self, item: dict[str, Any], strict: bool):
        super().__init__(item, strict=strict)

    @cached_property
    def prefix(self) -> str:
        return config.get("handle", {}).get("prefix", "")

    @cached_property
    def landing_page_url(self) -> str:
        return config.get("cmip6", {}).get("landing_page_url", "").rstrip("/")

    @cached_property
    def item_id(self) -> str:
        if "id" not in self.item:
            raise KeyError("Missing 'id' in item")
        return self.item["id"]

    @cached_property
    def assets(self) -> dict[str, Any]:
        return self.item.get("assets", {})

    def get_asset(self, key: str) -> dict[str, Any]:
        """Return the asset dictionary for a given key."""
        return self.assets.get(key, {})

    def get_asset_property(self, key: str, prop: str, default: Any = None) -> Any:
        """Return a property value from an asset, or a default if missing."""
        return self.get_asset(key).get(prop, default)

    @abstractmethod
    def pid(self) -> str:
        """Return the persistent identifier (PID) for this CMIP6 file or dataset record."""
        ...

    @cached_property
    def url(self) -> str:
        return f"{self.landing_page_url}/{self.prefix}/{self.pid}"
