from piddiplatsch.config import config
from piddiplatsch.lookup.base import AbstractLookup
from piddiplatsch.lookup.stac import STACLookup


class DummyLookup(AbstractLookup):
    def find_versions(self, query: dict) -> list[str]:
        return []


def get_lookup() -> AbstractLookup:
    """
    Factory function that returns the configured lookup instance
    based on the TOML config [lookup] section.
    """
    backend = config.get("lookup", "backend", "stac").lower()

    if backend == "stac":
        stac_url = config.get("stac", "base_url")
        collection = config.get("lookup", "collection", "cmip6")
        if not stac_url:
            raise ValueError("STAC backend requires 'stac_url' in config")
        return STACLookup(stac_url=stac_url, collection=collection)

    elif backend == "dummy":
        return DummyLookup()

    else:
        raise ValueError(f"Unknown lookup backend configured: {backend}")
