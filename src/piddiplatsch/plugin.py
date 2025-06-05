import pluggy

from .plugins.cmip6_processor import CMIP6Processor


hookspec = pluggy.HookspecMarker("piddiplatsch")

class MessageProcessorSpec:
    @hookspec
    def process(self, key, value, handle_client) -> None:
        """Process the message."""

def load_processor():
    pm = pluggy.PluginManager("piddiplatsch")
    pm.add_hookspecs(MessageProcessorSpec)

    pm.register(CMIP6Processor())

    # Ensure exactly one processor is registered
    processors = list(pm.get_plugins())
    if len(processors) != 1:
        raise RuntimeError("Exactly one processor plugin must be configured.")

    return processors[0]
