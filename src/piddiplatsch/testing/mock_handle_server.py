from flask import Flask, request, jsonify
import logging

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = app.logger

# In-memory handle store
handles = {}

# Constants
HANDLE_PREFIX = "21.T11148"
DUMMY_USERNAME_HANDLE = f"{HANDLE_PREFIX}/testuser"
DUMMY_USERNAME = f"300:{DUMMY_USERNAME_HANDLE}"
DUMMY_PASSWORD = "testpass"

# Admin handle record
ADMIN_HANDLE_RECORD = {
    "handle": DUMMY_USERNAME_HANDLE,
    "values": [
        {
            "index": 100,
            "type": "HS_ADMIN",
            "data": {
                "format": "admin",
                "value": {
                    "index": "200",
                    "handle": f"0.NA/{HANDLE_PREFIX}",
                    "permissions": "011111110011",
                },
            },
        }
    ],
}

# Preload admin user handle
handles[DUMMY_USERNAME_HANDLE] = ADMIN_HANDLE_RECORD


# Expose reset function for test cleanup
def _reset_handles():
    global handles
    handles = {}

    # Restore admin record
    handles[DUMMY_USERNAME_HANDLE] = ADMIN_HANDLE_RECORD


@app.route("/api/handles/<prefix>/<suffix>", methods=["GET"])
def get_handle(prefix, suffix):
    handle = f"{prefix}/{suffix}"
    logger.debug(f"GET request for handle: {handle}")

    record = handles.get(handle)
    if record:
        logger.debug(f"Handle found: {handle}")
        return jsonify({**record, "handle": handle, "responseCode": 1}), 200
    else:
        logger.debug(f"Handle not found: {handle}")
        return (
            jsonify({"message": f"Handle {handle} not found", "responseCode": 100}),
            200,
        )


@app.route("/api/handles/<prefix>/<suffix>", methods=["PUT"])
def put_handle(prefix, suffix):
    handle = f"{prefix}/{suffix}"
    overwrite = request.args.get("overwrite", "false").lower() == "true"

    logger.debug(f"PUT request for handle: {handle}, overwrite={overwrite}")

    if handle in handles and not overwrite:
        logger.debug(f"Handle already exists: {handle}")
        return jsonify({"message": f"Handle {handle} already exists"}), 409

    try:
        data = request.get_json(force=True)
        if not isinstance(data, dict) or "values" not in data:
            raise ValueError("Invalid handle record format")
    except Exception as e:
        logger.error(f"Invalid request body: {e}")
        return jsonify({"message": f"Invalid request body: {e}"}), 400

    data["handle"] = handle  # Ensure handle is set correctly
    handles[handle] = data

    logger.debug(f"Handle registered: {handle} with data: {data}")
    return (
        jsonify(
            {
                "responseCode": 1,
                "handle": handle,
                "message": f"Handle {handle} registered",
            }
        ),
        200,
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
