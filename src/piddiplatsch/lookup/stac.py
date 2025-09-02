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
    version_number: int


def extract_version(item: Item) -> int:
    """Extract integer version number from CMIP6 dataset-id (vYYYYMMDD)."""
    props = split_cmip6_id(item.id)
    return props.version_number


def split_cmip6_id(item_id: str) -> Properties:
    """Split CMIP6 dataset-id into structured properties."""
    parts = item_id.split(".")
    if len(parts) < 10:
        raise ValueError(f"Invalid CMIP6 dataset-id format: {item_id}")
    return Properties(
        activity_id=parts[1],
        institution_id=parts[2],
        source_id=parts[3],
        experiment_id=parts[4],
        variant_label=parts[5],
        table_id=parts[6],
        variable_id=parts[7],
        grid_label=parts[8],
        version=parts[-1],
        version_number=int(parts[-1][1:]),
    )


class STACLookup(AbstractLookup):
    """STAC-based implementation of AbstractLookup for CMIP6-style IDs."""

    def __init__(self, stac_url: str, collection: str = "cmip6"):
        self.client = Client.open(stac_url)
        self.collection = collection

    def find_versions(self, item_id: str) -> list[Item]:
        """Find all dataset versions, sorted with latest version first."""
        props = split_cmip6_id(item_id)
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
        items = list(search.items())
        return sorted(items, key=extract_version, reverse=True)  # latest first

    def previous_version(self, item_id: str) -> str | None:
        """Return the previous version before the given dataset, if any."""
        current = split_cmip6_id(item_id)
        items = self.find_versions(item_id)
        for item in items:
            if extract_version(item) < current.version_number:
                return item.id
        return None
