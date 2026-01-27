from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Protocol

from .processing import BaseProcessor


class ProcessorFactory(Protocol):
    def __call__(self, **kwargs) -> BaseProcessor:  # pragma: no cover
        ...


@dataclass(frozen=True)
class PluginSpec:
    """Declarative plugin specification.

    A plugin provides a name and a callable to build its processor. This keeps
    extension points simple: users add a module under ``piddiplatsch.plugins``
    that exposes a ``plugin`` attribute with this spec.
    """

    name: str
    make_processor: ProcessorFactory
    description: Optional[str] = None
