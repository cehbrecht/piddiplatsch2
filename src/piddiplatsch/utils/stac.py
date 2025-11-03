from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any
from multiformats import multihash

import requests
from pystac import Item

from piddiplatsch.config import config  # your TOML config loader


@dataclass
class Properties:
    activity_id: str
    institution_id: str
    source_id: str
    experiment_id: str
    variant_label: str
    table_id: str
    variable_id: str
    grid_label: str
    version: str
    version_number: int


def split_cmip6_id(item_id: str) -> Properties:
    """Split CMIP6 dataset-id into structured properties."""
    parts = item_id.split(".")
    if len(parts) < 10:
        raise ValueError(
            f"Invalid CMIP6 dataset-id format: {item_id}. "
            f"Expected format: <project>.<activity_id>.<institution_id>.<source_id>."
            f"<experiment_id>.<variant_label>.<table_id>.<variable_id>.<grid_label>.<version>"
        )
    return Properties(
        activity_id=parts[1],
        institution_id=parts[2],
        source_id=parts[3],
        experiment_id=parts[4],
        variant_label=parts[5],
        table_id=parts[6],
        variable_id=parts[7],
        grid_label=parts[8],
        version=parts[-1],
        version_number=int(parts[-1][1:]),
    )

def parse_multihash_hex(checksum_with_type: str) -> tuple[str, str]:
    """
    Parse a multihash hex string into (checksum_type, checksum_hex).

    Args:
        checksum_with_type: multihash as hex string, e.g.
            '12205994471abb01112afcc18159f6cc74b4f511b99806da59b3caf5a9c173cacfc5'

    Returns:
        Tuple of:
        - checksum_type: human-readable hash name, e.g., 'sha2-256'
        - checksum_hex: raw digest as hex string
    """
    # Convert hex string to bytes
    mh_bytes = bytes.fromhex(checksum_with_type)

    # Decode the multihash
    code, digest_bytes = multihash.unwrap_raw(mh_bytes)

    # Get human-readable hash name
    checksum_type = multihash.get(code).name

    # Convert digest bytes to hex
    checksum_hex = digest_bytes.hex()

    return checksum_type, checksum_hex


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
