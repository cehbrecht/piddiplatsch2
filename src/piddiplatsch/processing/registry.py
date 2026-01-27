"""Simple registry with declarative plugin discovery.

Supports two extension mechanisms:
- Manual registration via `register_processor(name, cls)`
- Declarative plugins exposing `plugin: PluginSpec` under `piddiplatsch.plugins.*`

Example plugin module:
    from piddiplatsch.core import PluginSpec
    from .processor import MyProcessor

    plugin = PluginSpec(
        name="mydomain",
        make_processor=lambda **kwargs: MyProcessor(**kwargs),
        description="My domain processor",
    )
"""

from importlib import import_module
from pkgutil import iter_modules

from piddiplatsch.core.processing import BaseProcessor
from piddiplatsch.core.plugin import PluginSpec

# Registry of available processors
_PROCESSORS: dict[str, type[BaseProcessor]] = {}


def register_processor(name: str, processor_class: type[BaseProcessor]) -> None:
    """Register a processor class under a given name.

    Args:
        name: Unique identifier for the processor (e.g., 'cmip6', 'cordex')
        processor_class: The processor class to register
    """
    _PROCESSORS[name] = processor_class


def get_processor(name: str, **kwargs) -> BaseProcessor:
    """Get an instance of a registered processor.

    Args:
        name: Name of the processor to instantiate
        **kwargs: Arguments to pass to the processor constructor

    Returns:
        Instance of the requested processor

    Raises:
        ValueError: If processor name is not registered
    """
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


# Auto-register built-in processors
def _discover_plugins() -> None:
    """Discover and register plugins declaring `plugin: PluginSpec`.

    Scans the `piddiplatsch.plugins` package for submodules that expose a
    `plugin` attribute matching `PluginSpec`. Safe to call multiple times.
    """
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
            # Register the processor factory by wrapping into a type for registry
            # The registry expects a class, but we can adapt by creating a tiny
            # subclass on the fly if needed. Simpler: store factory under name.
            # Here we register a proxy class that delegates construction to factory.
            factory = spec.make_processor

            class _FactoryProxy(BaseProcessor):
                def __init__(self, **kwargs):
                    # Replace self with factory-built instance; proxy holds nothing
                    obj = factory(**kwargs)
                    self.__dict__ = obj.__dict__
                    self.__class__ = obj.__class__

                def process(self, key: str, value: dict) -> "ProcessingResult":  # pragma: no cover
                    raise NotImplementedError

            register_processor(spec.name, _FactoryProxy)


# Discover and register plugins on module import
_discover_plugins()
