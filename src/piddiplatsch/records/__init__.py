from typing import TYPE_CHECKING

__all__ = ["CMIP6DatasetRecord", "CMIP6FileRecord"]

if TYPE_CHECKING:
    # For type checkers only; avoids runtime circular imports
    from piddiplatsch.plugins.cmip6.record import (
        CMIP6DatasetRecord as CMIP6DatasetRecord,
    )
    from piddiplatsch.plugins.cmip6.record import CMIP6FileRecord as CMIP6FileRecord


def __getattr__(name: str):
    if name == "CMIP6DatasetRecord":
        from piddiplatsch.plugins.cmip6.record import CMIP6DatasetRecord

        return CMIP6DatasetRecord
    if name == "CMIP6FileRecord":
        from piddiplatsch.plugins.cmip6.record import CMIP6FileRecord

        return CMIP6FileRecord
    raise AttributeError(name)
