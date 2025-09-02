"""
STAC-based lookup implementation for CMIP6-style dataset IDs.

This module provides functionality to query a STAC API for all versions
of a dataset, sorted by version (latest first).

Example:
    >>> from piddiplatsch.lookup.stac import STACLookup
    >>> lookup = STACLookup("https://stac.example.org", collection="cmip6")
    >>> query = {
    ...     "activity_id": {"eq": "ScenarioMIP"},
    ...     "institution_id": {"eq": "UA"},
    ...     "source_id": {"eq": "MCM-UA-1-0"},
    ...     "experiment_id": {"eq": "ssp585"},
    ...     "variant_label": {"eq": "r1i1p1f2"},
    ...     "table_id": {"eq": "Amon"},
    ...     "variable_id": {"eq": "pr"},
    ...     "grid_label": {"eq": "gn"},
    ... }
    >>> versions = lookup.find_versions(query)
    >>> print(versions[0])  # latest version first
    'CMIP6.ScenarioMIP.UA.MCM-UA-1-0.ssp585.r1i1p1f2.Amon.pr.gn.v20220101'
"""

from pystac import Item
from pystac_client import Client

from piddiplatsch.lookup.base import AbstractLookup


def extract_version(item: Item) -> int:
    """Extract the integer version number from a CMIP6 dataset-id (e.g., vYYYYMMDD)."""
    return int(item.id.split(".")[-1][1:])


class STACLookup(AbstractLookup):
    """STAC-based implementation of AbstractLookup for CMIP6-style IDs."""

    def __init__(self, stac_url: str, collection: str = "cmip6") -> None:
        self.client = Client.open(stac_url)
        self.collection = collection

    def find_versions(self, query: dict) -> list[str]:
        """Return all dataset versions matching the query, sorted with latest version first."""
        search = self.client.search(collections=[self.collection], query=query)
        items = list(search.items())
        return [item.id for item in sorted(items, key=extract_version, reverse=True)]
