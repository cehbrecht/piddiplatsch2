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
    """Load and return the one configured message processor plugin."""
    plugin_name = config.get("plugins", "processor", fallback=None)
    if not plugin_name:
        raise RuntimeError("No processor plugin configured under [plugins].processor")

    try:
        # Dynamically import the configured plugin
        module = __import__(plugin_name, fromlist=["plugin"])
    except ImportError as e:
        raise RuntimeError(f"Could not import plugin '{plugin_name}': {e}")

    # Set up pluggy and register the plugin
    pm = pluggy.PluginManager("piddiplatsch")
    pm.add_hookspecs(MessageProcessorSpec)
    pm.register(module)

    # Check if the plugin implements the required methods
    hook_caller = pm.hook
    if not hook_caller.can_process or not hook_caller.process:
        raise RuntimeError(
            f"Plugin '{plugin_name}' does not implement required methods."
        )

    # Ensure only one plugin is registered and usable
    processors = pm.get_plugins()
    if len(processors) != 1:
        raise RuntimeError(
            f"Expected exactly one processor plugin, found {len(processors)}"
        )

    return module
