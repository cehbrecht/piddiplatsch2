import uuid
import warnings
from typing import Any
from pydantic import BaseModel, Field, ValidationError, RootModel
from pydantic_core import PydanticCustomError


class CMIP6HandleModel(BaseModel):
    PID: uuid.UUID
    URL: str
    AGGREGATION_LEVEL: str = "Dataset"
    DATASET_ID: str
    DATASET_VERSION: str | None = None
    HOSTING_NODE: str = "unknown"
    REPLICA_NODE: list[str] = Field(default_factory=list)
    UNPUBLISHED_REPLICAS: list[str] = Field(default_factory=list)
    UNPUBLISHED_HOSTS: list[str] = Field(default_factory=list)


class CMIP6HandleRecord:
    def __init__(self, item: dict[str, Any]):
        try:
            id_ = item["id"]
            dataset_id, version = CMIP6HandleRecord._split_id(id_)
            pid = uuid.uuid3(uuid.NAMESPACE_URL, id_)
            url = CMIP6HandleRecord._extract_url(item)
            hosting_node = CMIP6HandleRecord._extract_hosting_node(item)

            self._model = CMIP6HandleModel(
                PID=pid,
                URL=url,
                DATASET_ID=dataset_id,
                DATASET_VERSION=version,
                HOSTING_NODE=hosting_node,
            )
        except KeyError as e:
            raise ValueError(f"Missing required item field: {e}") from e
        except ValidationError as e:
            raise ValueError(f"Invalid handle record: {e}") from e

    # --- Read-only accessors
    @property
    def pid(self) -> uuid.UUID:
        return self._model.PID

    @property
    def url(self) -> str:
        return self._model.URL

    @property
    def hostname(self) -> str:
        return self._model.HOSTING_NODE

    def as_record(self) -> dict[str, Any]:
        return self._model.model_dump()

    def as_json(self) -> str:
        return self._model.model_dump_json(indent=2)

    def to_handle_record(self) -> list[dict]:
        """Convert to pyhandle-compatible format."""
        fields = []
        index = 1

        def add_field(field_type: str, value: str):
            nonlocal index
            fields.append({
                "index": index,
                "type": field_type,
                "data": {"format": "string", "value": value}
            })
            index += 1

        m = self._model
        add_field("PID", str(m.PID))
        add_field("URL", m.URL)
        add_field("AGGREGATION_LEVEL", m.AGGREGATION_LEVEL)
        add_field("DATASET_ID", m.DATASET_ID)

        if m.DATASET_VERSION:
            add_field("DATASET_VERSION", m.DATASET_VERSION)
        if m.HOSTING_NODE:
            add_field("HOSTING_NODE", m.HOSTING_NODE)

        for v in m.REPLICA_NODE:
            add_field("REPLICA_NODE", v)
        for v in m.UNPUBLISHED_REPLICAS:
            add_field("UNPUBLISHED_REPLICAS", v)
        for v in m.UNPUBLISHED_HOSTS:
            add_field("UNPUBLISHED_HOSTS", v)

        return fields

    # --- Static helpers
    @staticmethod
    def _split_id(id_: str) -> tuple[str, str | None]:
        parts = id_.rsplit(".", 1)
        if len(parts) == 2:
            return parts[0], parts[1]
        return id_, None

    @staticmethod
    def _extract_url(item: dict[str, Any]) -> str:
        try:
            return item["links"][0]["href"]
        except (KeyError, IndexError) as e:
            raise ValueError("Missing 'links[0].href' in item") from e

    @staticmethod
    def _extract_hosting_node(item: dict[str, Any]) -> str:
        assets = item.get("assets", {})
        ref_node = assets.get("reference_file", {}).get("alternate:name")
        data_node = assets.get("data0001", {}).get("alternate:name")

        if ref_node:
            return ref_node
        elif data_node:
            warnings.warn("Using fallback data0001 as hosting node", stacklevel=2)
            return data_node
        else:
            warnings.warn("No hosting node found, using 'unknown'", stacklevel=2)
            return "unknown"
