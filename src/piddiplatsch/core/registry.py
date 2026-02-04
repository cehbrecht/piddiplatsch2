"""Processor registry for explicitly registered plugins.

Supports:
- Manual registration via `register_processor(name, cls)`
- Static default registrations (e.g., CMIP6)
"""

from .processing import BaseProcessor

# Registry of available processors
_PROCESSORS: dict[str, type[BaseProcessor]] = {}


def register_processor(name: str, processor_class: type[BaseProcessor]) -> None:
    """Register a processor class under a given name."""
    _PROCESSORS[name] = processor_class


def get_processor(name: str, **kwargs) -> BaseProcessor:
    """Get an instance of a registered processor."""
    if name not in _PROCESSORS:
        available = list(_PROCESSORS.keys())
        raise ValueError(
            f"Processor '{name}' not found. Available processors: {available}"
        )

    processor_class = _PROCESSORS[name]
    return processor_class(**kwargs)


def list_processors() -> list[str]:
    """Return list of registered processor names."""
    return list(_PROCESSORS.keys())


# Static default registrations on module import
try:
    from piddiplatsch.plugins.cmip6.processor import CMIP6Processor

    register_processor("cmip6", CMIP6Processor)
except Exception:
    # In environments where plugins are not available, registry remains empty
    pass
