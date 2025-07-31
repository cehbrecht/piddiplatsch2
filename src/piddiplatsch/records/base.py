import logging
from abc import ABC, abstractmethod
from typing import Any

from jsonschema import ValidationError, validate
from pydantic import BaseModel

from piddiplatsch.records.utils import drop_empty


class BaseRecord(ABC):
    """Abstract base class for Handle records wrapping a dict item with validation."""

    def __init__(
        self, item: dict[str, Any], schema: dict | None = None, strict: bool = False
    ):
        self.item = item
        self.strict = strict
        self.schema = schema
        self.validate()  # call the validation method

    def validate(self):
        """Validate the item against the JSON schema if given."""
        if self.schema:
            try:
                validate(instance=self.item, schema=self.schema)
            except ValidationError as e:
                logging.error(
                    "Schema validation failed at %s: %s",
                    list(e.absolute_path),
                    e.message,
                )
                raise ValueError(f"Invalid item: {e.message}") from e

    @abstractmethod
    def as_handle_model(self) -> BaseModel:
        """Return a Pydantic model representing the handle record."""
        ...

    def as_record(self) -> dict:
        """Return the handle model as a dict with empty fields dropped."""
        return drop_empty(self.as_handle_model().model_dump())

    def as_json(self) -> str:
        """Return the handle model as JSON string."""
        return self.as_handle_model().model_dump_json()

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} item_id={self.item.get('id', None)}>"
