import pluggy

from piddiplatsch.plugins.cmip6_processor import CMIP6Processor
from piddiplatsch.result import ProcessingResult

PLUGIN_NAMESPACE = "piddiplatsch"

hookspec = pluggy.HookspecMarker(PLUGIN_NAMESPACE)


class MessageProcessorSpec:
    @hookspec
    def process(self, key: str, value: dict, handle_client: object) -> ProcessingResult:
        """Process a single Kafka message."""
