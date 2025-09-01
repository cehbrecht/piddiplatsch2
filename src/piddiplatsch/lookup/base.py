from abc import ABC, abstractmethod
from typing import Any


class AbstractLookup(ABC):
    """Abstract base class for dataset version lookup mechanisms."""

    @abstractmethod
    def find_versions(self, dataset_id: str) -> list[Any]:
        """Return all versions of a dataset."""
        pass

    @abstractmethod
    def latest_version(self, dataset_id: str) -> tuple[Any, str, str] | None:
        """
        Return the latest version of a dataset.

        Returns:
            (dataset_object, base_id, version) or None if not found.
        """
        pass

    @abstractmethod
    def latest_previous_version(self, dataset_id: str) -> tuple[Any, str, str] | None:
        """
        Return the latest previous version of a dataset.

        Returns:
            (dataset_object, base_id, version) or None if not found.
        """
        pass

    @abstractmethod
    def is_latest(self, dataset_id: str) -> bool:
        """Check if the given dataset-id is the latest version."""
        pass
