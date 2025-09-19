"""
Elasticsearch-based lookup implementation for dataset versions via handles.
"""

import re

from elasticsearch import Elasticsearch

from piddiplatsch.exceptions import LookupError
from piddiplatsch.lookup.base import AbstractLookup
from piddiplatsch.utils.stac import extract_version


class ElasticsearchLookup(AbstractLookup):
    """Elasticsearch-based implementation of AbstractLookup for dataset versions."""

    HANDLE_PATTERN = re.compile(r"^\d+\.\w+/.+$")

    def __init__(self, es_url: str, index: str = "handle_21t14995") -> None:
        self.es_url = es_url
        self.index = index
        self._client: Elasticsearch | None = None

    def _get_client(self) -> Elasticsearch:
        """Lazy initialization of the Elasticsearch client."""
        if self._client is None:
            try:
                self._client = Elasticsearch(self.es_url)
            except Exception as e:
                raise LookupError(
                    f"Could not connect to Elasticsearch at {self.es_url}: {e}"
                ) from e
        return self._client

    def _validate_handle(self, handle_value: str) -> None:
        """Ensure the handle has a plausible prefix/suffix structure."""
        if not isinstance(handle_value, str):
            raise LookupError(f"Handle must be a string, got {type(handle_value)}")
        if not self.HANDLE_PATTERN.match(handle_value):
            raise LookupError(f"Invalid handle format: {handle_value}")

    def find_versions(self, handle_value: str, limit: int = 100) -> list[str]:
        """Return dataset versions linked to a given handle, sorted latest-first.

        Args:
            handle_value: Handle string (e.g., "21.T14995/<UUID>").
            limit: Maximum number of versions to return. Defaults to 100.

        Raises:
            LookupError: If the Elasticsearch service is unavailable, the query fails,
                         or the handle format is invalid.
        """
        self._validate_handle(handle_value)

        query = {"query": {"term": {"handle.keyword": handle_value}}}

        try:
            client = self._get_client()
            # size=limit ensures we do not fetch more docs than needed
            resp = client.search(index=self.index, body=query, size=limit)
        except Exception as e:
            raise LookupError(f"Elasticsearch query failed: {e}") from e

        hits = resp.get("hits", {}).get("hits", [])
        if not hits:
            return []

        # Collect unique DATASET_VERSION values
        versions = {
            h["_source"]["data"]
            for h in hits
            if h["_source"].get("type") == "DATASET_VERSION"
        }

        return sorted(versions, key=extract_version, reverse=True)[:limit]
