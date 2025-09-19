from elasticsearch import Elasticsearch

from piddiplatsch.exceptions import LookupError
from piddiplatsch.lookup.base import AbstractLookup
from piddiplatsch.utils.models import build_handle, item_pid
from piddiplatsch.utils.stac import extract_version


class ElasticsearchLookup(AbstractLookup):
    """Elasticsearch-based implementation of AbstractLookup for dataset versions."""

    def __init__(self, es_url: str, index: str = "handle_21t14995") -> None:
        self.es_url = es_url
        self.index = index
        self._client: Elasticsearch | None = None

    def _get_client(self) -> Elasticsearch:
        if self._client is None:
            try:
                self._client = Elasticsearch(self.es_url)
            except Exception as e:
                raise LookupError(
                    f"Could not connect to Elasticsearch at {self.es_url}: {e}"
                ) from e
        return self._client

    def find_versions(self, item_id: str, limit: int = 100) -> list[str]:
        """
        Return all full dataset IDs of previous versions for a given item_id.

        Args:
            item_id: CMIP6 dataset-id (full ID).
            limit: Maximum number of versions to return (default 100).

        Returns:
            List of full dataset IDs, sorted latest-first.

        Raises:
            LookupError: If the Elasticsearch query fails.
        """
        handle_value = build_handle(item_pid(item_id), as_uri=True)
        query = {"query": {"term": {"handle.keyword": handle_value}}}

        try:
            resp = self._get_client().search(index=self.index, body=query, size=limit)
        except Exception as e:
            raise LookupError(f"Elasticsearch query failed: {e}") from e

        hits = resp.get("hits", {}).get("hits", [])
        if not hits:
            return []

        # extract versions from hits
        versions = {
            h["_source"]["data"]
            for h in hits
            if h["_source"].get("type") == "DATASET_VERSION"
        }

        # reconstruct full dataset IDs
        base_id = item_id.rsplit(".", 1)[0]
        full_ids = [f"{base_id}.{v}" for v in versions]

        return sorted(full_ids, key=extract_version, reverse=True)[:limit]
