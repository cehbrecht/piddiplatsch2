from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel, ValidationError

from piddiplatsch.records.utils import drop_empty


class BaseRecord(ABC):
    def __init__(self, item: dict[str, Any], strict: bool):
        self.item = item
        self.strict = strict
        # Optionally validate using Pydantic model if you want
        # self.validate()

    @abstractmethod
    def as_handle_model(self) -> BaseModel:
        """Return a Pydantic model representing the handle record."""
        ...

    def validate(self):
        """Validate the Pydantic model constructed from item."""
        try:
            self.as_handle_model().model_validate()
        except ValidationError as e:
            # Log or raise as needed
            raise ValueError(f"Pydantic validation failed: {e}") from e

    def as_record(self) -> dict:
        return drop_empty(self.as_handle_model().model_dump())

    def as_json(self) -> str:
        return self.as_handle_model().model_dump_json()

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} id={self.item.get('id', None)}>"
