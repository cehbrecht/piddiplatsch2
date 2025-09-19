from elasticsearch import Elasticsearch

from piddiplatsch.exceptions import LookupError
from piddiplatsch.lookup.base import AbstractLookup
from piddiplatsch.utils.stac import extract_version, split_cmip6_id


def build_dataset_id(item_id: str) -> str:
    props = split_cmip6_id(item_id)
    dataset_id = ".".join(  # noqa: FLY002
        [
            "CMIP6",
            props.activity_id,
            props.institution_id,
            props.source_id,
            props.experiment_id,
            props.variant_label,
            props.table_id,
            props.variable_id,
            props.grid_label,
        ]
    )
    return dataset_id


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

    def _build_query(self, item_id: str, limit: int = 100) -> dict:
        """Build Elasticsearch query for a dataset_id derived from item_id."""

        dataset_id = build_dataset_id(item_id)

        return {"query": {"term": {"data.keyword": dataset_id}}, "size": limit}

    def find_versions(self, item_id: str, limit: int = 100) -> list[str]:
        """Find all versions of a CMIP6 dataset based on DATASET_ID."""

        query_body = self._build_query(item_id, limit=limit)
        dataset_id = build_dataset_id(item_id)

        try:
            resp = self._get_client().search(index=self.index, body=query_body)
        except Exception as e:
            raise LookupError(f"Elasticsearch query failed: {e}") from e

        hits = resp.get("hits", {}).get("hits", [])
        if not hits:
            return []

        dataset_versions = []
        seen = set()
        for hit in hits:
            source = hit.get("_source", {})
            if source.get("type") != "DATASET_VERSION":
                continue

            version_str = source.get("data")
            if not version_str or version_str in seen:
                continue

            full_id = f"{dataset_id}.{version_str}"
            dataset_versions.append(full_id)
            seen.add(version_str)

        dataset_versions.sort(key=extract_version, reverse=True)
        return dataset_versions[:limit]
