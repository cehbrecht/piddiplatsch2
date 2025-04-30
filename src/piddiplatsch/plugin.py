import pluggy
from piddiplatsch.config import config

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
    pm = pluggy.PluginManager("piddiplatsch")
    pm.add_hookspecs(MessageProcessorSpec)

    # Dynamically import the configured plugin
    processor_path = config.get("processor", "module")
    module = __import__(processor_path, fromlist=[""])
    processor = getattr(module, "processor")

    pm.register(processor)

    # Ensure exactly one processor is registered
    hook_caller = pm.hook
    implementations = list(pm.get_plugins())
    if len(implementations) != 1:
        raise RuntimeError("Exactly one processor plugin must be configured.")

    return processor  # ✅ return the actual processor instance, not `pm.hook`
