import pluggy
from piddiplatsch import config

hookspec = pluggy.HookspecMarker("piddiplatsch")
hookimpl = pluggy.HookimplMarker("piddiplatsch")


class MessageProcessorSpec:
    @hookspec
    def can_process(self, key, value) -> bool:
        """Return True if this processor can handle the message."""

    @hookspec
    def process(self, key, value, handle_client) -> None:
        """Process the message."""


def load_processor():
    """Load and return the plugin hook caller for the configured processor."""
    print("load_processor")
    plugin_name = config.get("plugins", "processor", fallback=None)
    if not plugin_name:
        raise RuntimeError("No processor plugin configured under [plugins].processor")

    try:
        module = __import__(plugin_name, fromlist=["plugin"])
    except ImportError as e:
        raise RuntimeError(f"Could not import plugin '{plugin_name}': {e}")

    pm = pluggy.PluginManager("piddiplatsch")
    pm.add_hookspecs(MessageProcessorSpec)
    pm.register(module)

    processors = pm.get_plugins()
    if len(processors) != 1:
        raise RuntimeError(
            f"Expected exactly one processor plugin, found {len(processors)}"
        )

    return pm.hook
