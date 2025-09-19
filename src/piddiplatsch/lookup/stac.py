"""
STAC-based lookup implementation for CMIP6-style dataset IDs.
"""

from typing import Any

from pystac_client import Client, ItemSearch

from piddiplatsch.exceptions import LookupError
from piddiplatsch.lookup.base import AbstractLookup
from piddiplatsch.utils.stac import extract_version


class STACLookup(AbstractLookup):
    """STAC-based implementation of AbstractLookup for CMIP6-style IDs."""

    def __init__(self, stac_url: str, collection: str = "cmip6") -> None:
        self.stac_url = stac_url
        self.collection = collection
        self._client: Client | None = None

    def _get_client(self) -> Client:
        """Lazy initialization of the STAC client."""
        if self._client is None:
            try:
                self._client = Client.open(self.stac_url)
            except Exception as e:
                raise LookupError(
                    f"Could not connect to STAC service {self.stac_url}: {e}"
                ) from e
        return self._client

    def _validate_query(self, query: dict[str, Any]) -> None:
        """Optional lightweight validation of query structure."""
        if not isinstance(query, dict):
            raise LookupError(f"STAC query must be a dict, got {type(query)}")
        # Example: enforce CMIP6-specific fields
        required_keys = ["cmip6:source_id", "cmip6:institution_id"]
        missing = [k for k in required_keys if k not in query]
        if missing:
            import warnings

            warnings.warn(
                f"Query missing recommended CMIP6 keys: {missing}", stacklevel=2
            )

    def find_versions(self, query: dict, limit: int = 100) -> list[str]:
        """Return dataset versions matching the query, sorted with latest version first.

        Args:
            query: STAC query dict.
            limit: Maximum number of versions to return. Defaults to 100.

        Raises:
            LookupError: If the STAC service is unavailable or the query fails.
        """
        self._validate_query(query)

        try:
            client = self._get_client()
            search: ItemSearch = client.search(
                collections=[self.collection],
                query=query,
                max_items=limit,  # pystac-client handles paging under the hood
            )
            items = list(search.items())
        except Exception as e:
            raise LookupError(f"STAC query failed: {e}") from e

        if not items:
            return []

        # Sort by extracted version, latest first
        return [
            item.id for item in sorted(items, key=extract_version, reverse=True)[:limit]
        ]
