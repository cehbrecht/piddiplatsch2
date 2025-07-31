from abc import ABC, abstractmethod

from pydantic import BaseModel

from piddiplatsch.records.utils import drop_empty


class BaseRecord(ABC):
    """Abstract base class for Handle records with common serialization methods."""

    @abstractmethod
    def as_handle_model(self) -> BaseModel:
        """Return a Pydantic model representing the handle record."""
        ...

    def as_record(self) -> dict:
        """Return the handle model as a dict (with empty fields dropped)."""
        return drop_empty(self.as_handle_model().model_dump())

    def as_json(self) -> str:
        """Return the handle model as a JSON string."""
        return self.as_handle_model().model_dump_json()
