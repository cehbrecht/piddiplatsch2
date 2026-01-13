from abc import ABC, abstractmethod


class AbstractLookup(ABC):
    """Abstract base class for dataset version lookup mechanisms."""

    @abstractmethod
    def find_versions(self, item_id: str, limit: int = 100) -> list[str]:
        """
        Return all versions of a dataset, sorted with latest version first.

        Args:
            item_id: The dataset identifier (e.g., CMIP6 dataset-id).
            limit: Maximum number of versions to return. Defaults to 100.

        Returns:
            List of item IDs or dataset versions, sorted latest-first.

        Raises:
            LookupError: If the lookup fails.
        """
        pass

class DummyLookup(AbstractLookup):
    """A no-op lookup that returns empty results, ignores all init arguments."""

    def __init__(self, **kwargs):
        pass

    def find_versions(self, item_id: str, limit: int = 100) -> list[str]:
        return []
