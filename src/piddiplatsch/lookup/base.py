from abc import ABC, abstractmethod
from typing import Any


class AbstractLookup(ABC):
    """Abstract base class for dataset version lookup mechanisms."""

    @abstractmethod
    def find_versions(self, dataset_id: str) -> list[Any]:
        """Return all versions of a dataset."""
        pass

    @abstractmethod
    def previous_version(self, dataset_id: str) -> str | None:
        """
        Return the previous version of a dataset.

        Returns:
            (dataset_object, base_id, version) or None if not found.
        """
        pass
