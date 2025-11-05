import json
import logging
from datetime import datetime
from typing import Any

import pyhandle
import urllib3
from pyhandle.clientcredentials import PIDClientCredentials
from pyhandle.handleexceptions import HandleAlreadyExistsException

from piddiplatsch.config import config
from piddiplatsch.utils.models import build_handle

from .base import HandleBackend

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def _prepare_handle_data(record: dict[str, Any]) -> dict[str, str]:
    """Prepare handle record fields: serialize list/dict values, skip None, convert datetime."""
    prepared: dict[str, str] = {}

    for key, value in record.items():
        if value is None:
            continue

        if isinstance(value, list | dict):

            def serialize(obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                return obj

            value = json.dumps(value, default=serialize)
        elif isinstance(value, datetime):
            value = value.isoformat()

        prepared[key] = value

    return prepared


class HandleClient(HandleBackend):
    """PyHandle client implementing HandleBackend interface."""

    def __init__(
        self,
        server_url: str,
        prefix: str,
        username: str,
        password: str,
        verify_https: bool = False,
    ):
        handle_cfg = {
            "client": "rest",
            "handle_server_url": server_url,
            "username": username,
            "password": password,
            "HTTPS_verify": verify_https,
            "prefix": prefix,
        }
        cred = PIDClientCredentials(**handle_cfg)
        self.client = pyhandle.handleclient.PyHandleClient(
            "rest"
        ).instantiate_with_credentials(cred)

    @classmethod
    def from_config(cls) -> "HandleClient":
        return cls(
            server_url=config.get("handle", "server_url"),
            prefix=config.get("handle", "prefix"),
            username=config.get("handle", "username"),
            password=config.get("handle", "password"),
        )

    def add(self, pid: str, record: dict[str, Any]) -> None:
        handle = build_handle(pid)
        handle_data = _prepare_handle_data(record)
        location = handle_data.pop("URL", None)
        if not location:
            raise ValueError("Missing required 'URL' in record")

        try:
            self.client.register_handle(
                handle=handle,
                location=location,
                overwrite=True,
                **handle_data,
            )
        except HandleAlreadyExistsException:
            pass
        except Exception as e:
            logging.error(f"Failed to register handle {handle}: {e}")
            raise

    def get(self, pid: str) -> dict | None:
        handle = build_handle(pid)
        try:
            response = self.client.retrieve_handle_record_json(handle)
            if not response or "values" not in response:
                return None

            record: dict[str, Any] = {}
            for entry in response["values"]:
                key = entry.get("type")
                value = entry.get("data")

                if key in ("HS_ADMIN", None):
                    continue

                try:
                    record[key] = json.loads(value)
                except (TypeError, json.JSONDecodeError):
                    record[key] = value

            return record

        except pyhandle.handleexceptions.HandleNotFoundException:
            return None
        except Exception as e:
            logging.error(f"Error retrieving handle {handle}: {e}")
            raise

    def update(self, pid: str, record: dict[str, Any]) -> None:
        handle = build_handle(pid)
        handle_data = _prepare_handle_data(record)
        location = handle_data.pop("URL", None)
        self.client.modify_handle(handle=handle, location=location, **handle_data)
