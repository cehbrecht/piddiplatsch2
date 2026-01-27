"""Processor registry with declarative plugin discovery.

Supports two extension mechanisms:
- Manual registration via `register_processor(name, cls)`
- Declarative plugins exposing `plugin: PluginSpec` under `piddiplatsch.plugins.*`
"""

from importlib import import_module
from pkgutil import iter_modules

from .plugin import PluginSpec
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


def _discover_plugins() -> None:
    """Discover and register plugins declaring `plugin: PluginSpec`."""
    try:
        import piddiplatsch.plugins as plugins_pkg
    except ImportError:
        return

    prefix = plugins_pkg.__name__ + "."
    for mod in iter_modules(plugins_pkg.__path__, prefix):
        name = mod.name
        try:
            module = import_module(name)
        except Exception:
            continue

        spec = getattr(module, "plugin", None)
        if isinstance(spec, PluginSpec):
            factory = spec.make_processor

            class _FactoryProxy(BaseProcessor):
                def __init__(self, __factory=factory, **kwargs):
                    obj = __factory(**kwargs)
                    self.__dict__ = obj.__dict__
                    self.__class__ = obj.__class__

            register_processor(spec.name, _FactoryProxy)


# Discover and register plugins on module import
_discover_plugins()
