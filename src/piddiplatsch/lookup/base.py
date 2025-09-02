from abc import ABC, abstractmethod


class AbstractLookup(ABC):
    """Abstract base class for dataset version lookup mechanisms."""

    @abstractmethod
    def find_versions(self, query: dict) -> list[str]:
        """Return all versions of a dataset."""
        pass
