import pluggy
import logging
import uuid
from jsonschema import validate, ValidationError

hookimpl = pluggy.HookimplMarker("piddiplatsch")

# Define the required fields from a CMIP6 STAC item
CMIP6_ITEM_SCHEMA = {
    "type": "object",
    "required": ["id", "links", "properties", "assets"],
    "properties": {
        "id": {"type": "string"},
        "links": {
            "type": "array",
            "minItems": 1,
            "items": {"type": "object", "required": ["href"], "properties": {"href": {"type": "string"}}},
        },
        # "properties": {
            # "type": "object",
            # "required": ["version"],
            # "properties": {"version": {"type": "string"}},
        # },
        "assets": {
            "type": "object",
            "required": ["reference_file"],
            "properties": {
                "reference_file": {
                    "type": "object",
                    "required": ["alternate:name"],
                    "properties": {"alternate:name": {"type": "string"}},
                }
            },
        },
    },
}


class CMIP6Processor:
    @hookimpl
    def process(self, key, value, handle_client):
        logging.info(f"CMIP6 plugin processing key: {key}")

        try:
            item = value["data"]["payload"]["item"]
        except KeyError:
            raise ValueError("Missing 'item' in Kafka message")

        try:
            validate(instance=item, schema=CMIP6_ITEM_SCHEMA)
        except ValidationError as e:
            raise ValueError(f"Invalid CMIP6 STAC item: {e.message}")

        pid = item.get("id") or str(uuid.uuid5(uuid.NAMESPACE_DNS, key))
        url = item["links"][0]["href"]
        version = item["properties"]["version"]
        hosting_node = item["assets"]["reference_file"]["alternate:name"]

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
