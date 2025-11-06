import logging
from typing import Any

import pyhandle
import urllib3
from pyhandle.clientcredentials import PIDClientCredentials
from pyhandle.handleexceptions import HandleAlreadyExistsException

from piddiplatsch.config import config
from piddiplatsch.handles.base import HandleBackend

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


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

    def _store(self, handle: str, handle_data: dict[str, Any]) -> None:
        location = handle_data.pop("URL", None)
        try:
            self.client.register_handle(
                handle=handle,
                location=location,
                overwrite=True,
                **handle_data,
            )
        except HandleAlreadyExistsException:
            logging.debug("Handle already exists (ignored with overwrite=True)")
        except Exception as e:
            logging.error(f"Failed to register handle {handle}: {e}")
            raise
