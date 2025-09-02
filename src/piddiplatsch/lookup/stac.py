from pystac import Item
from pystac_client import Client

from piddiplatsch.lookup.base import AbstractLookup


def extract_version(item: Item) -> int:
    """Extract integer version number from CMIP6 dataset-id (vYYYYMMDD)."""
    parts = item.id.split(".")
    version_number = int(parts[-1][1:])
    return version_number


class STACLookup(AbstractLookup):
    """STAC-based implementation of AbstractLookup for CMIP6-style IDs."""

    def __init__(self, stac_url: str, collection: str = "cmip6"):
        self.client = Client.open(stac_url)
        self.collection = collection

    def find_versions(self, query: dict) -> list[str]:
        """Find all dataset versions, sorted with latest version first."""
        search = self.client.search(collections=[self.collection], query=query)
        items = list(search.items())
        items_sorted = sorted(items, key=extract_version, reverse=True)  # latest first
        item_ids = [item.id for item in items_sorted]
        return item_ids
