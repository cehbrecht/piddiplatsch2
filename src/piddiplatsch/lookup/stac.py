from pystac_client import Client

from piddiplatsch.exceptions import LookupError
from piddiplatsch.lookup.base import AbstractLookup
from piddiplatsch.utils.stac import extract_version, split_cmip6_id


class STACLookup(AbstractLookup):
    """STAC-based implementation of AbstractLookup for CMIP6-style IDs."""

    def __init__(self, stac_url: str, collection: str = "cmip6") -> None:
        self.stac_url = stac_url
        self.collection = collection
        self._client: Client | None = None

    def _get_client(self) -> Client:
        if self._client is None:
            try:
                self._client = Client.open(self.stac_url)
            except Exception as e:
                raise LookupError(
                    f"Could not connect to STAC at {self.stac_url}: {e}"
                ) from e
        return self._client

    def _build_query(self, item_id: str) -> dict:
        props = split_cmip6_id(item_id)
        return {
            "activity_id": {"eq": props.activity_id},
            "institution_id": {"eq": props.institution_id},
            "source_id": {"eq": props.source_id},
            "experiment_id": {"eq": props.experiment_id},
            "variant_label": {"eq": props.variant_label},
            "table_id": {"eq": props.table_id},
            "variable_id": {"eq": props.variable_id},
            "grid_label": {"eq": props.grid_label},
        }

    def find_versions(self, item_id: str, limit: int = 100) -> list[str]:
        query_dict = self._build_query(item_id)
        try:
            search = self._get_client().search(
                collections=[self.collection], query=query_dict
            )
            items = list(search.items())
        except Exception as e:
            raise LookupError(f"STAC query failed: {e}") from e

        return [
            item.id for item in sorted(items, key=extract_version, reverse=True)[:limit]
        ]
