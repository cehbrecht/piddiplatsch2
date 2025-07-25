import logging
import pyhandle
from piddiplatsch.config import config


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

    def build_handle(self, pid: str):
        """Build a full handle by combining the prefix and the PID."""
        return f"{self.prefix}/{pid}"

    def add_item(self, pid: str, record: dict):
        """Add a new PID to the Handle Service, supporting multi-valued fields like HAS_PARTS."""
        handle = self.build_handle(pid)

        # Extract location if present
        location = record.get(
            "URL", "https://example.org/placeholder"
        )  # fallback required by pyhandle

        # Build list of handle values
        handle_data = []
        for key, value in record.items():
            if isinstance(value, list):
                for v in value:
                    handle_data.append({"type": key, "parsed_data": v})
            else:
                handle_data.append({"type": key, "parsed_data": value})

        try:
            self.client.register_handle(
                handle=handle,
                location=location,
                list_of_entries=handle_data,
                overwrite=True,
            )
            logging.debug(f"Added handle: {handle}")
        except pyhandle.handleexceptions.HandleAlreadyExistsException:
            logging.warning(f"Handle already exists: {handle}")
        except Exception as e:
            logging.error(f"Failed to register handle {handle}: {e}")
            raise

    def get_item(self, pid: str) -> dict | None:
        """Retrieve a PID record as a dict of {type: value}. Returns None if not found."""
        handle = self.build_handle(pid)

        try:
            response = self.client.retrieve_handle_record_json(handle)
            if not response or "values" not in response:
                return None

            result = {}
            for entry in response["values"]:
                key = entry.get("type")
                data = entry.get("data")

                # Handle both string and dict formats
                if isinstance(data, dict):
                    value = data.get("value", data)
                else:
                    value = data

                result[key] = value

            return result

        except pyhandle.handleexceptions.HandleNotFoundException:
            logging.warning(f"Handle not found: {handle}")
            return None
        except Exception as e:
            logging.error(f"Error retrieving handle {handle}: {e}")
            raise
