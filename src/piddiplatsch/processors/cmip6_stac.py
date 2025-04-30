import logging
from piddiplatsch.plugin import MessageProcessorSpec


class CMIP6StacProcessor:
    def can_process(self, key, value):
        """Check if the processor can handle the message."""
        # Add logic to determine if this processor can handle the message
        return "cmip6" in value.get("type", "")

    def process(self, key, value, handle_client):
        """Process the message."""
        logging.info(f"Processing CMIP6 message: {key}")

        pid = value["data"]["payload"]["item"]["id"]
        record = {
            "URL": value["data"]["payload"]["item"]["links"][0]["href"],
            "CHECKSUM": None,
        }

        try:
            handle_client.add_item(pid, record)
            logging.info(f"Added item: pid = {pid}")
        except Exception as e:
            logging.error(f"Failed to add item with pid = {pid}: {e}")
