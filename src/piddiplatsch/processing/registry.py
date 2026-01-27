"""Simple processor registry for managing different data processors.

This replaces the pluggy-based plugin system with a straightforward registry
that's easy to extend for future processors (CORDEX, etc.).

Usage:
    # Get an existing processor
    processor = get_processor("cmip6", dry_run=True)

    # Register a new processor
    from piddiplatsch.processing import BaseProcessor

    class CORDEXProcessor(BaseProcessor):
        def process(self, key: str, value: dict) -> ProcessingResult:
            # Your processing logic here
            pass

    register_processor("cordex", CORDEXProcessor)

    # Then use it like any other processor
    processor = get_processor("cordex")
"""

from piddiplatsch.processing import BaseProcessor

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
def _register_builtin_processors():
    """Register all built-in processors."""
    from piddiplatsch.plugins.cmip6.processor import CMIP6Processor

    register_processor("cmip6", CMIP6Processor)


# Register built-in processors on module import
_register_builtin_processors()
