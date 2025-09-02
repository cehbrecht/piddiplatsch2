from abc import ABC, abstractmethod
from typing import Any

import requests
from pystac import Item

from piddiplatsch.config import config  # your TOML config loader


def extract_version(item: Item) -> int:
    """Extract the integer version number from a CMIP6 dataset-id (e.g., vYYYYMMDD)."""
    return int(item.id.split(".")[-1][1:])


class BaseStacClient(ABC):
    @abstractmethod
    def get_item(self, collection_id: str, item_id: str) -> dict[str, Any]:
        """Fetch a full STAC item."""
        pass


class RemoteStacClient(BaseStacClient):
    def __init__(self, base_url: str, timeout: float = 10.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def get_item(self, collection_id: str, item_id: str) -> dict[str, Any]:
        url = f"{self.base_url}/collections/{collection_id}/items/{item_id}"
        resp = requests.get(url, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()


class LocalStacClient(BaseStacClient):
    """Dummy/local STAC client for testing."""

    def get_item(self, collection_id: str, item_id: str) -> dict[str, Any]:
        return {
            "id": item_id,
            "type": "Feature",
            "collection": collection_id,
            "properties": {},
            "links": [],
            "assets": {},
        }


def get_stac_client() -> BaseStacClient:
    """
    Factory function to return a STAC client based on the config.

    Config example:
    [stac]
    base_url = "https://api.stac.esgf.ceda.ac.uk"
    """
    stac_cfg = config.get("stac", {})
    base_url = stac_cfg.get("base_url")
    timeout = float(stac_cfg.get("timeout", 10.0))
    if not base_url:
        return LocalStacClient()
    return RemoteStacClient(base_url, timeout=timeout)
