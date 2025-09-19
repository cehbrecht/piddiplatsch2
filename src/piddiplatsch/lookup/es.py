import re

from elasticsearch import Elasticsearch

from piddiplatsch.exceptions import LookupError
from piddiplatsch.lookup.base import AbstractLookup
from piddiplatsch.utils.models import build_handle, item_pid
from piddiplatsch.utils.stac import extract_version


class ElasticsearchLookup(AbstractLookup):
    """Elasticsearch-based implementation of AbstractLookup for dataset versions."""

    HANDLE_PATTERN = re.compile(r"^\d+\.\w+/.+$")

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
        Return full dataset IDs for all versions of the given item.

        Combines DATASET_ID and DATASET_VERSION from handle records.
        Eliminates duplicates and sorts by version descending.
        """
        handle_pid = build_handle(item_pid(item_id), as_uri=True)
        query = {"query": {"term": {"handle.keyword": handle_pid}}}

        try:
            resp = self._get_client().search(index=self.index, body=query, size=limit)
        except Exception as e:
            raise LookupError(f"Elasticsearch query failed: {e}") from e

        hits = resp.get("hits", {}).get("hits", [])
        if not hits:
            return []

        seen = set()
        full_ids = []

        for h in hits:
            values = h["_source"].get("values", [])
            dataset_id = next(
                (v["data"]["value"] for v in values if v["type"] == "DATASET_ID"),
                None,
            )
            dataset_version = next(
                (v["data"]["value"] for v in values if v["type"] == "DATASET_VERSION"),
                None,
            )
            if dataset_id and dataset_version:
                full_id = f"{dataset_id}.{dataset_version}"
                if full_id not in seen:
                    seen.add(full_id)
                    full_ids.append(full_id)

        # Sort by version descending and apply limit
        return sorted(full_ids, key=extract_version, reverse=True)[:limit]
