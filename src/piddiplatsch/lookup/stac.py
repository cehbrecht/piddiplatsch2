from pystac import Item
from pystac_client import Client

from .base import AbstractLookup


class STACLookup(AbstractLookup):
    """STAC-based implementation of AbstractLookup for CMIP6-style IDs."""

    def __init__(self, stac_url: str, collection: str = "cmip6"):
        self.client = Client.open(stac_url)
        self.collection = collection

    @staticmethod
    def split_cmip6_id(dataset_id: str) -> tuple[str, dict, str]:
        parts = dataset_id.split(".")
        if len(parts) < 10:
            raise ValueError(f"Invalid CMIP6 dataset-id format: {dataset_id}")
        version = parts[-1]
        properties = {
            "activity_id": parts[1],
            "institution_id": parts[2],
            "source_id": parts[3],
            "experiment_id": parts[4],
            "variant_label": parts[5],
            "table_id": parts[6],
            "variable_id": parts[7],
            "grid_label": parts[8],
        }
        base_id = ".".join(parts[:-1])
        return base_id, properties, version

    @staticmethod
    def base_dataset_id(dataset_id: str) -> str:
        base_id, _, _ = STACLookup.split_cmip6_id(dataset_id)
        return base_id

    @staticmethod
    def previous_version_link(item: Item | None, href: str | None) -> dict:
        """
        Return a STAC-style link dict for a previous version.
        If item or href is None, returns a placeholder link.
        """
        if item is None or href is None:
            return {
                "rel": "previous",
                "href": None,
                "type": "application/json",
                "title": "No previous version",
            }
        return {
            "rel": "previous",
            "href": href,
            "type": "application/json",
            "title": f"Previous version of {item.id}",
        }

    def find_versions(self, dataset_id: str) -> list[Item]:
        _, props, _ = self.split_cmip6_id(dataset_id)
        search = self.client.search(collections=[self.collection], query=props)
        return list(search.items())

    def latest_version(self, dataset_id: str) -> tuple[Item, str, str] | None:
        items = self.find_versions(dataset_id)
        if not items:
            return None

        def extract_version(item: Item) -> int:
            return int(item.id.split(".")[-1][1:])

        latest_item = max(items, key=extract_version)
        base_id, _, version = self.split_cmip6_id(latest_item.id)
        return latest_item, base_id, version

    def latest_previous_version(self, dataset_id: str) -> tuple[Item, str, str] | None:
        _, _, current_version = self.split_cmip6_id(dataset_id)
        current_v_int = int(current_version[1:])
        items = self.find_versions(dataset_id)
        previous_items = [
            item for item in items if int(item.id.split(".")[-1][1:]) < current_v_int
        ]
        if not previous_items:
            return None

        latest_prev_item = max(
            previous_items, key=lambda i: int(i.id.split(".")[-1][1:])
        )
        base_id, _, version = self.split_cmip6_id(latest_prev_item.id)
        return latest_prev_item, base_id, version

    def latest_previous_version_link(self, dataset_id: str, base_href: str) -> dict:
        """
        Return a STAC-style link dict for the latest previous version.
        Returns a placeholder link if no previous version exists.
        """
        result = self.latest_previous_version(dataset_id)
        if not result:
            return self.previous_version_link(None, None)
        item, _, _ = result
        href = f"{base_href}/{item.id}.json"
        return self.previous_version_link(item, href)

    def is_latest(self, dataset_id: str) -> bool:
        _, _, version = self.split_cmip6_id(dataset_id)
        latest = self.latest_version(dataset_id)
        if not latest:
            return False
        _, _, latest_version = latest
        return version == latest_version
