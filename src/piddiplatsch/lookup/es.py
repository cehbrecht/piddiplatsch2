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
        Return all versions of a dataset as full dataset_ids, sorted by version_number descending.
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

        dataset_ids = set()
        for hit in hits:
            values = hit.get("_source", {}).get("values", [])
            dataset_id = None
            dataset_version = None

            for v in values:
                type_value = v.get("type")
                data_value = v.get("data", {}).get("value")
                if type_value == "DATASET_ID":
                    dataset_id = data_value
                elif type_value == "DATASET_VERSION":
                    dataset_version = data_value

            if dataset_id and dataset_version:
                full_id = f"{dataset_id}.{dataset_version}"
                dataset_ids.add(full_id)

        # Sort by version_number descending
        return sorted(dataset_ids, key=extract_version, reverse=True)[:limit]
