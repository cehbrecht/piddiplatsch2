import logging
import uuid
from typing import Any, Dict, List, Optional

from pydantic import ValidationError
from piddiplatsch.handle.models import CMIP6HandleRecordModel

logger = logging.getLogger(__name__)


class HandleRecordError(ValueError):
    pass


class CMIP6HandleRecord:
    def __init__(self, item: Dict[str, Any], *, strict: bool = False):
        self._strict = strict
        try:
            identifier = item["id"]
            pid = uuid.uuid3(uuid.NAMESPACE_URL, identifier)
            dataset_id, version = self._split_id(identifier)
            url = self._extract_url(item)
            hosting_node = self._extract_hosting_node(item)
            replicas = self._extract_replicas(item)

            self._model = CMIP6HandleRecordModel(
                pid=pid,
                url=url,
                dataset_id=dataset_id,
                dataset_version=version,
                hosting_node=hosting_node,
                replica_node=replicas,
            )

        except (KeyError, IndexError, TypeError) as e:
            logger.error("Missing or invalid field in item: %s", e)
            raise HandleRecordError(f"Invalid input structure: {e}") from e
        except ValidationError as e:
            if strict:
                raise HandleRecordError(f"Validation failed: {e}") from e
            logger.warning("Validation warning: %s", e)
            self._model = self._fallback_model(
                pid=pid,
                url=url,
                dataset_id=dataset_id,
                version=version,
                hosting_node=hosting_node,
                replicas=replicas,
            )

    @staticmethod
    def _split_id(identifier: str) -> tuple[str, Optional[str]]:
        parts = identifier.rsplit(".", 1)
        return parts[0], parts[1] if len(parts) > 1 else None

    @staticmethod
    def _extract_url(item: Dict[str, Any]) -> str:
        for link in item.get("links", []):
            if "href" in link:
                return link["href"]
        raise ValueError("No 'href' link found")

    @staticmethod
    def _extract_hosting_node(item: Dict[str, Any]) -> str:
        assets = item.get("assets", {})
        for key in ["reference_file", "data0001"]:
            name = assets.get(key, {}).get("alternate:name")
            if name:
                return name
        for asset in assets.values():
            if "alternate:name" in asset:
                return asset["alternate:name"]
        logger.warning("Hosting node not found, defaulting to 'unknown'")
        return "unknown"

    @staticmethod
    def _extract_replicas(item: Dict[str, Any]) -> List[str]:
        names = []
        for asset in item.get("assets", {}).values():
            name = asset.get("alternate:name")
            if name and name not in names:
                names.append(name)
        return names

    def _fallback_model(
        self,
        *,
        pid: uuid.UUID,
        url: str,
        dataset_id: str,
        version: Optional[str],
        hosting_node: str,
        replicas: List[str],
    ) -> CMIP6HandleRecordModel:
        """Builds a model bypassing strict validation for non-essential fields."""
        return CMIP6HandleRecordModel.model_construct(
            PID=pid,
            URL=url,
            AGGREGATION_LEVEL="Dataset",
            DATASET_ID=dataset_id,
            DATASET_VERSION=version,
            HOSTING_NODE=hosting_node,
            REPLICA_NODE=replicas,
            UNPUBLISHED_REPLICAS=[],
            UNPUBLISHED_HOSTS=[],
        )

    @property
    def pid(self) -> uuid.UUID:
        return self._model.PID

    def as_record(self) -> Dict[str, Any]:
        return self._model.model_dump(by_alias=True)

    def as_json(self) -> str:
        return self._model.model_dump_json(indent=2, by_alias=True)
