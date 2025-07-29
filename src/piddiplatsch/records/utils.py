import logging
from typing import Dict, Any, List
from .cmip6_asset_record import CMIP6FileRecord


def extract_asset_records(
    item: Dict[str, Any], exclude_keys: List[str] = None
) -> List[CMIP6FileRecord]:
    """
    Given a CMIP6 STAC item, return a list of CMIP6AssetRecord instances
    for all asset keys except those in exclude_keys.

    Args:
        item: A CMIP6 STAC item as a dict.
        exclude_keys: Optional list of asset keys to ignore (e.g., ["thumbnail", "quicklook"]).

    Returns:
        A list of CMIP6AssetRecord objects.
    """
    exclude_keys = set(exclude_keys or [])
    assets = item.get("assets", {})

    records = []
    for key in assets:
        if key in exclude_keys:
            continue
        try:
            record = CMIP6FileRecord(item, key)
            records.append(record)
        except ValueError as e:
            # Log and skip problematic assets
            logging.warning(f"Skipping asset '{key}': {e}")
    return records
