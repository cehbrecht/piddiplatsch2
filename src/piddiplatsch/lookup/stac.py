from pystac import Item
from pystac_client import Client

from piddiplatsch.lookup.base import AbstractLookup


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
            "activity_id": {"eq": parts[1]},
            "institution_id": {"eq": parts[2]},
            "source_id": {"eq": parts[3]},
            "experiment_id": {"eq": parts[4]},
            "variant_label": {"eq": parts[5]},
            "table_id": {"eq": parts[6]},
            "variable_id": {"eq": parts[7]},
            "grid_label": {"eq": parts[8]},
        }
        base_id = ".".join(parts[:-1])
        return base_id, properties, version

    @staticmethod
    def base_dataset_id(dataset_id: str) -> str:
        base_id, _, _ = STACLookup.split_cmip6_id(dataset_id)
        return base_id

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

        # latest_prev_item = max(
        #    previous_items, key=lambda i: int(i.id.split(".")[-1][1:])
        # )
        return previous_items[0]

    def is_latest(self, dataset_id: str) -> bool:
        _, _, version = self.split_cmip6_id(dataset_id)
        latest = self.latest_version(dataset_id)
        if not latest:
            return False
        _, _, latest_version = latest
        return version == latest_version
