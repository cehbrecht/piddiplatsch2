import logging
from functools import lru_cache
import pyhandle


class HandleServiceClient:
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

    def build_handle(self, pid):
        """Build a full handle by combining the prefix and the PID."""
        return f"{self.prefix}/{pid}"

    def add_pid(self, record):
        """Add a new PID to the Handle Service."""
        if "pid" not in record:
            raise ValueError("Record must contain a 'pid' field.")

        pid = record["pid"]
        handle = self.build_handle(pid)
        
        # Get location from the record if available
        location = record.get("location", None)

        try:
            # Register the handle with the given location (if available) and overwrite flag
            self.client.register_handle(
                handle=handle, location=location, overwrite=True, **record
            )
            logging.info(f"Added PID {handle} for record: {record}")
        except pyhandle.handleexceptions.HandleAlreadyExistsException:
            logging.info(f"Handle already exists: {handle}")
        except Exception as e:
            logging.error(f"Failed to register PID {handle}: {e}")
            raise

    def update_pid(self, record):
        """Update an existing PID."""
        pid = record.get("pid")
        if not pid:
            raise ValueError("Missing 'pid' in record.")
        
        handle = self.build_handle(pid)
        
        self.client.modify_handle(handle, record)
        logging.info(f"Updated PID {handle} for record: {record}")

    def delete_pid(self, pid):
        """Delete a PID from the Handle Service."""
        handle = self.build_handle(pid)
        
        self.client.delete_handle(handle)
        logging.info(f"Deleted PID: {handle}")

    @lru_cache(maxsize=1000)
    def lookup_pid(self, pid):
        """Lookup a PID in the Handle Service (cached)."""
        handle = self.build_handle(pid)
        
        try:
            handle_data = self.client.retrieve_handle_record(handle)
            logging.info(f"Found PID {handle}: {handle_data}")
            return handle_data
        except Exception:
            logging.info(f"PID {handle} not found.")
            return None
