from dataclasses import dataclass

from pystac import Item
from pystac_client import Client

from piddiplatsch.lookup.base import AbstractLookup


@dataclass
class Properties:
    activity_id: str
    institution_id: str
    source_id: str
    experiment_id: str
    variant_label: str
    table_id: str
    variable_id: str
    grid_label: str
    version: str


def extract_version(item: Item) -> int:
    return int(item.id.split(".")[-1][1:])


def split_cmip6_id(dataset_id: str) -> tuple[str, dict, str]:
    parts = dataset_id.split(".")
    if len(parts) < 10:
        raise ValueError(f"Invalid CMIP6 dataset-id format: {dataset_id}")
    props = Properties(
        activity_id=parts[1],
        institution_id=parts[2],
        source_id=parts[3],
        experiment_id=parts[4],
        variant_label=parts[5],
        table_id=parts[6],
        variable_id=parts[7],
        grid_label=parts[8],
        version=parts[-1],
    )
    return props


class STACLookup(AbstractLookup):
    """STAC-based implementation of AbstractLookup for CMIP6-style IDs."""

    def __init__(self, stac_url: str, collection: str = "cmip6"):
        self.client = Client.open(stac_url)
        self.collection = collection

    def find_versions(self, dataset_id: str) -> list[Item]:
        props = split_cmip6_id(dataset_id)
        query = {
            "activity_id": {"eq": props.activity_id},
            "institution_id": {"eq": props.institution_id},
            "source_id": {"eq": props.source_id},
            "experiment_id": {"eq": props.experiment_id},
            "variant_label": {"eq": props.variant_label},
            "table_id": {"eq": props.table_id},
            "variable_id": {"eq": props.variable_id},
            "grid_label": {"eq": props.grid_label},
        }
        search = self.client.search(collections=[self.collection], query=query)
        return list(search.items())

    def latest_version(self, dataset_id: str) -> str | None:
        items = self.find_versions(dataset_id)
        if not items:
            return None

        latest_item = max(items, key=extract_version)
        return latest_item.id

    def previous_version(self, dataset_id: str) -> str | None:
        props = split_cmip6_id(dataset_id)
        current_version = int(props.version[1:])
        items = self.find_versions(dataset_id)
        previous_items = [
            item for item in items if extract_version(item) < current_version
        ]
        if not previous_items:
            return None

        prev_item = max(previous_items, key=extract_version)
        return prev_item.id

    def is_latest(self, dataset_id: str) -> bool:
        props = split_cmip6_id(dataset_id)
        latest = self.latest_version(dataset_id)
        if not latest:
            return False
        return props.version == latest
