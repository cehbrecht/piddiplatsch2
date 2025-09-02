"""
STAC-based lookup implementation for CMIP6-style dataset IDs.
"""

from pystac_client import Client

from piddiplatsch.exceptions import LookupError
from piddiplatsch.lookup.base import AbstractLookup
from piddiplatsch.utils.stac import extract_version


class STACLookup(AbstractLookup):
    """STAC-based implementation of AbstractLookup for CMIP6-style IDs."""

    def __init__(self, stac_url: str, collection: str = "cmip6") -> None:
        self.client = Client.open(stac_url)
        self.collection = collection

    def find_versions(self, query: dict) -> list[str]:
        """Return all dataset versions matching the query, sorted with latest version first.

        Raises:
            LookupError: If the STAC service is unavailable or the query fails.
        """
        try:
            search = self.client.search(collections=[self.collection], query=query)
            items = list(search.items())
        except Exception as e:
            raise LookupError(f"STAC query failed: {e}") from e

        return [item.id for item in sorted(items, key=extract_version, reverse=True)]
