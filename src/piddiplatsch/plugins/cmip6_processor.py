import pluggy
import logging
import uuid

hookimpl = pluggy.HookimplMarker("piddiplatsch")

class CMIP6Processor:

    @hookimpl
    def process(self, key, value, handle_client):
        logging.info(f"CMIP6 plugin processing key: {key}")

        item = value["data"]["payload"]["item"]

        try:
            pid = item["id"]
        except KeyError:
            pid = str(uuid.uuid5(uuid.NAMESPACE_DNS, key))

        try:
            url = item["links"][0]["href"]
        except (KeyError, IndexError):
            raise ValueError("Missing URL in message")

        try:
            version = item["properties"]["version"]
        except (KeyError, IndexError):
            raise ValueError("Missing VERSION in message")

        try:
            hosting_node = item["assets"]["reference_file"]["alternate:name"]
        except (KeyError, IndexError):
            raise ValueError("Missing HOSTING_NODE in message")

        record = {
            "URL": url,
            "CHECKSUM": None,
            "AGGREGATION_LEVEL": "Dataset",
            "DATASET_ID": pid,
            "DATASET_VERSION": version,
            "HOSTING_NODE": hosting_node,
            "REPLICA_NODE": "",
            "UNPUBLISHED_REPLICAS": "",
            "UNPUBLISHED_HOSTS": "",
        }
        handle_client.add_item(pid, record)
