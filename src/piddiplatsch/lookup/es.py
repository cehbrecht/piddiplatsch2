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
        handle_value = build_handle(item_pid(item_id), as_uri=True)

        query = {"query": {"term": {"handle.keyword": handle_value}}}

        try:
            resp = self._get_client().search(index=self.index, body=query, size=limit)
        except Exception as e:
            raise LookupError(f"Elasticsearch query failed: {e}") from e

        hits = resp.get("hits", {}).get("hits", [])
        if not hits:
            return []

        versions = {
            h["_source"]["data"]
            for h in hits
            if h["_source"].get("type") == "DATASET_VERSION"
        }

        return sorted(versions, key=extract_version, reverse=True)[:limit]
