import logging
import pyhandle
from piddiplatsch.config import config

# Use logger for this module
logger = logging.getLogger(__name__)


class HandleClient:
    def __init__(self, server_url, prefix, username, password, verify_https=False):
        self.server_url = server_url
        self.prefix = prefix
        self.verify_https = verify_https
        client = pyhandle.handleclient.PyHandleClient("rest")
        client.instantiate_with_username_and_password(
            handle_server_url=server_url,
            username=f"300:{prefix}/{username}",
            password=password,
            HTTPS_verify=verify_https,
        )

        # set rest client
        self.client = client.handle_client

        # Patch internal connector for testing purposes
        connector = self.client._RESTHandleClient__handlesystemconnector
        connector._HandleSystemConnector__has_write_access = True
        connector._HandleSystemConnector__handle_server_url = server_url
        connector._HandleSystemConnector__HTTPS_verify = verify_https
        connector._HandleSystemConnector__authentication_method = "user_pw"
        connector._HandleSystemConnector__basic_authentication_string = "_noauth_"

    @classmethod
    def from_config(cls):
        """Create and return a HandleClient instance using configured credentials."""
        return cls(
            server_url=config.get("handle", "server_url"),
            prefix=config.get("handle", "prefix"),
            username=config.get("handle", "username"),
            password=config.get("handle", "password"),
        )

    def build_handle(self, pid):
        """Build a full handle by combining the prefix and the PID."""
        return f"{self.prefix}/{pid}"

    def add_item(self, pid, record):
        """Add a new PID to the Handle Service."""
        handle = self.build_handle(pid)

        try:
            # Register the handle and overwrite flag
            self.client.register_handle(
                handle=handle, location=record["URL"], overwrite=True, **record
            )
            logger.info(f"Added handle: {handle}")
        except pyhandle.handleexceptions.HandleAlreadyExistsException:
            logger.info(f"Handle already exists: {handle}")
        except Exception as e:
            logger.error(f"Failed to register handle {handle}: {e}")
            raise
