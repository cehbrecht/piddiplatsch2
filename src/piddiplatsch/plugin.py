import pluggy

from piddiplatsch.result import ProcessingResult

PLUGIN_NAMESPACE = "piddiplatsch"

hookspec = pluggy.HookspecMarker(PLUGIN_NAMESPACE)


class MessageProcessorSpec:
    @hookspec
    def process(self, key: str, value: dict) -> ProcessingResult:
        """Process a single Kafka message."""
