import pluggy
import logging
import uuid

hookspec = pluggy.HookspecMarker("piddiplatsch")
hookimpl = pluggy.HookimplMarker("piddiplatsch")


class MessageProcessorSpec:
    @hookspec
    def process(self, key, value, handle_client) -> None:
        """Process the message."""


class CMIP6Processor:

    @hookimpl
    def process(self, key, value, handle_client):
        logging.info(f"CMIP6 plugin processing key: {key}")
        try:
            pid = value["data"]["payload"]["item"]["id"]
        except KeyError:
            pid = str(uuid.uuid5(uuid.NAMESPACE_DNS, key))

        try:
            url = value["data"]["payload"]["item"]["links"][0]["href"]
        except (KeyError, IndexError):
            raise ValueError("Missing URL in message")

        record = {"URL": url, "CHECKSUM": None}
        handle_client.add_item(pid, record)


def load_processor():
    pm = pluggy.PluginManager("piddiplatsch")
    pm.add_hookspecs(MessageProcessorSpec)

    pm.register(CMIP6Processor())

    # Ensure exactly one processor is registered
    processors = list(pm.get_plugins())
    if len(processors) != 1:
        raise RuntimeError("Exactly one processor plugin must be configured.")

    return processors[0]
