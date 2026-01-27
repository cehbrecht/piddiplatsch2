from __future__ import annotations

from datetime import datetime

from pydantic import (
    Field,
    HttpUrl,
    PositiveInt,
    PrivateAttr,
    field_serializer,
    model_validator,
)

from piddiplatsch.plugins.cmip6.model import CMIP6DatasetModel, CMIP6FileModel

__all__ = ["CMIP6DatasetModel", "CMIP6FileModel"]
