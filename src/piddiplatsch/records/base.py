from abc import ABC, abstractmethod
from functools import cached_property
from typing import Any

from pydantic import BaseModel, ValidationError

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

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} id={self.item.get('id', 'UNKNOWN')}>"
