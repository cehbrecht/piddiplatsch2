from flask import Flask, request, jsonify, make_response
from functools import wraps
import base64

import logging

app = Flask(__name__)

# Enable logging
logging.basicConfig(level=logging.DEBUG)

handles = {}  # In-memory handle store

# Dummy admin user credentials
HANDLE_PREFIX = "21.T11148"
DUMMY_USERNAME_HANDLE = f"{HANDLE_PREFIX}/testuser"
DUMMY_USERNAME = f"300:{DUMMY_USERNAME_HANDLE}"
DUMMY_PASSWORD = "testpass"

# Admin handle record template
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
                    "permissions": "011111110011"
                }
            }
        }
    ]
}

def check_auth(auth_header):
    return True

    # if not auth_header or not auth_header.startswith("Basic "):
    #     return False
    # encoded = auth_header.split(" ")[1]
    # decoded = base64.b64decode(encoded).decode()
    # return decoded == f"{DUMMY_USERNAME}:{DUMMY_PASSWORD}"

def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth_header = request.headers.get("Authorization")
        if not check_auth(auth_header):
            return make_response(jsonify({"message": "Unauthorized"}), 401)
        return f(*args, **kwargs)
    return decorated

@app.route("/api/handles/<prefix>/<suffix>", methods=["GET"])
def get_handle(prefix, suffix):
    handle = f"{prefix}/{suffix}"

    app.logger.debug(f"get_handle: handle={handle}")

    if handle == DUMMY_USERNAME_HANDLE:
        response = dict(ADMIN_HANDLE_RECORD)  # Make a copy
        response["responseCode"] = 1          # Add the required field
        return jsonify(response), 200

    if handle in handles:
        app.logger.debug(f"get_handle: handle={handle} found")
        # Add responseCode to the response
        response = dict(handles[handle])  # Copy the stored record
        response["handle"] = handle       # Add the required key
        response["responseCode"] = 1      # Optional, useful for debugging
        
        return jsonify(response), 200  # Return with the added responseCode
        # return jsonify(handles[handle]), 200
    else:
        app.logger.debug(f"get_handle: handle={handle} not found")
        return jsonify({"message": f"Handle {handle} not found", "responseCode": 100}), 404

@app.route("/api/handles/<prefix>/<suffix>", methods=["PUT"])
@require_auth
def put_handle(prefix, suffix):
    handle = f"{prefix}/{suffix}"
    app.logger.debug(f"Received PUT for: {handle}")
    
    overwrite = request.args.get("overwrite", "false").lower() == "true"

    if handle in handles and not overwrite:
        return jsonify({"message": f"Handle {handle} already exists"}), 409

    data = request.get_json()
    handles[handle] = data
    return jsonify({"responseCode": 1, "handle": handle, "message": f"Handle {handle} registered"}), 201

@app.route("/api/handles/<prefix>/<suffix>", methods=["DELETE"])
@require_auth
def delete_handle(prefix, suffix):
    handle = f"{prefix}/{suffix}"
    if handle in handles:
        del handles[handle]
        return jsonify({"message": f"Handle {handle} deleted"}), 200
    return jsonify({"message": f"Handle {handle} not found"}), 404

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
