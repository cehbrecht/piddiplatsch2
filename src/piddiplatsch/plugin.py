import pluggy

from .plugins.cmip6_processor import CMIP6Processor

PLUGIN_NAMESPACE = "piddiplatsch"

hookspec = pluggy.HookspecMarker(PLUGIN_NAMESPACE)

class MessageProcessorSpec:
    @hookspec
    def process(self, key: str, value: dict, handle_client: object) -> None:
        """Process a single Kafka message."""