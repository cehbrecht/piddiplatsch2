from piddiplatsch.plugin import hookimpl
import logging
import uuid


@hookimpl
def can_process(key, value) -> bool:
    return "data" in value and "payload" in value["data"]


@hookimpl
def process(key, value, handle_client):
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
