import logging
import pyhandle
from piddiplatsch.config import config
import json
from datetime import datetime
from typing import Any
from pyhandle.handleexceptions import HandleAlreadyExistsException
from pyhandle.clientcredentials import PIDClientCredentials

import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def _prepare_handle_data(record: dict[str, Any]) -> dict[str, str]:
    """Prepare handle record fields: serialize list/dict values, skip None and convert datetime."""
    prepared = {}
    for key, value in record.items():
        if value is None:
            continue
        if isinstance(value, datetime):
            value = value.isoformat()
        elif isinstance(value, (list, dict)):
            # Recursively convert datetimes inside lists/dicts
            def serialize(obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                return obj

            value = json.dumps(value, default=serialize)
        prepared[key] = value
    return prepared


def _parse_handle_record(values: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Convert Handle record 'values' list into a flat dict.

    Deserializes JSON strings for list/dict fields where possible.
    """
    record = {}

    for entry in values:
        key = entry.get("type")
        value = entry.get("data")

        if key in ("HS_ADMIN", None):
            continue  # skip control/undefined values

        if isinstance(value, str):
            try:
                parsed = json.loads(value)
                record[key] = parsed
            except (json.JSONDecodeError, TypeError):
                record[key] = value
        else:
            record[key] = value

    return record


class HandleClient:
    def __init__(self, server_url, prefix, username, password, verify_https=False):
        self.prefix = prefix

        handle_cfg = {
            "client": "rest",
            "handle_server_url": server_url,
            "username": username,
            "password": password,
            "HTTPS_verify": verify_https,
            "prefix": self.prefix,
        }

        cred = PIDClientCredentials(**handle_cfg)
        self.client = pyhandle.handleclient.PyHandleClient(
            "rest"
        ).instantiate_with_credentials(cred)

    @classmethod
    def from_config(cls):
        """Create and return a HandleClient instance using configured credentials."""
        return cls(
            server_url=config.get("handle", "server_url"),
            prefix=config.get("handle", "prefix"),
            username=config.get("handle", "username"),
            password=config.get("handle", "password"),
        )

    def build_handle(self, pid: str):
        """Build a full handle by combining the prefix and the PID."""
        return f"{self.prefix}/{pid}"

    def add_record(self, pid: str, record: dict[str, Any]):
        """Add a new PID to the Handle Service."""
        handle = self.build_handle(pid)

        try:
            handle_data = _prepare_handle_data(record)

            location = handle_data.pop("URL", None)
            if not location:
                raise ValueError("Missing required 'URL' in record")

            self.client.register_handle(
                handle=handle,
                location=location,
                overwrite=True,
                **handle_data,
            )
            logging.debug(f"Added handle: {handle}")
        except HandleAlreadyExistsException:
            logging.warning(f"Handle already exists: {handle}")
        except Exception as e:
            logging.error(f"Failed to register handle {handle}: {e}")
            raise

    def get_record(self, pid: str) -> dict | None:
        """Retrieve a PID record as a dict of {type: value}. Returns None if not found."""
        handle = self.build_handle(pid)

        try:
            response = self.client.retrieve_handle_record_json(handle)
            if not response or "values" not in response:
                return None

            record = _parse_handle_record(response["values"])
            return record

        except pyhandle.handleexceptions.HandleNotFoundException:
            logging.warning(f"Handle not found: {handle}")
            return None
        except Exception as e:
            logging.error(f"Error retrieving handle {handle}: {e}")
            raise
