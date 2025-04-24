import logging
import pyhandle

# Enable logging
logging.basicConfig(level=logging.DEBUG)


class HandleClient:
    def __init__(self, server_url, prefix, username, password, verify_https=False):
        self.server_url = server_url
        self.prefix = prefix
        self.verify_https = verify_https
        self.client = pyhandle.handleclient.PyHandleClient("rest")
        self.client.instantiate_with_username_and_password(
            handle_server_url=server_url,
            username=f"300:{prefix}/{username}",
            password=password,
            HTTPS_verify=verify_https,
        )

        # Patch internal connector for testing purposes
        connector = self.client.handle_client._RESTHandleClient__handlesystemconnector
        connector._HandleSystemConnector__has_write_access = True
        connector._HandleSystemConnector__handle_server_url = server_url
        connector._HandleSystemConnector__HTTPS_verify = verify_https
        connector._HandleSystemConnector__authentication_method = 'user_pw'

    def build_handle(self, pid):
        """Build a full handle by combining the prefix and the PID."""
        return f"{self.prefix}/{pid}"

    def add_item(self, pid, record):
        """Add a new PID to the Handle Service."""
        handle = self.build_handle(pid)

        try:
            # Register the handle and overwrite flag
            self.client.register_handle(handle=handle, location='https://example.org/location/1001', overwrite=True)
            logging.info(f"Added handle: {handle}")
        except pyhandle.handleexceptions.HandleAlreadyExistsException:
            logging.info(f"Handle already exists: {handle}")
        except Exception as e:
            logging.error(f"Failed to register handle {handle}: {e}")
            raise
