from flask import Flask, request, jsonify
from functools import wraps

app = Flask(__name__)

# Dummy credentials
VALID_USERNAME = "21.T11148/testuser"
VALID_PASSWORD = "testpass"

# In-memory handle storage
handle_store = {}

# Add the auth handle itself so GET /api/handles/300:21.T11148/testuser works
auth_handle = VALID_USERNAME
handle_store[auth_handle] = [
    {
        "index": 100,
        "type": "HS_ADMIN",
        "data": {
            "format": "admin",
            "value": {
                "handle": auth_handle,
                "index": 200,
                "permissions": "111111111111"
            }
        }
    }
]

def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or auth.username != VALID_USERNAME or auth.password != VALID_PASSWORD:
            return jsonify({
                "responseCode": 401,
                "message": "Unauthorized"
            }), 401
        return f(*args, **kwargs)
    return decorated

@app.route("/api/handles/<path:handle>", methods=["PUT"])
@require_auth
def register_handle(handle):
    overwrite = request.args.get("overwrite", "false").lower() == "true"
    entries = request.get_json()

    if entries is None:
        return jsonify({
            "responseCode": 400,
            "message": "Missing JSON payload"
        }), 400

    if not overwrite and handle in handle_store:
        return jsonify({
            "responseCode": 409,
            "message": f"Handle {handle} already exists and overwrite is false"
        }), 409

    handle_store[handle] = entries
    return jsonify({
        "responseCode": 1,
        "handle": handle,
        "values": entries,
        "overwrite": overwrite
    }), 200

@app.route("/api/handles/<path:handle>", methods=["GET"])
def resolve_handle(handle):
    if handle not in handle_store:
        return jsonify({
            "responseCode": 404,
            "message": f"Handle {handle} not found"
        }), 404

    return jsonify({
        "responseCode": 1,
        "handle": handle,
        "values": handle_store[handle]
    }), 200

@app.route("/api/handles/<path:handle>", methods=["DELETE"])
@require_auth
def delete_handle(handle):
    if handle not in handle_store:
        return jsonify({
            "responseCode": 404,
            "message": f"Handle {handle} not found"
        }), 404

    del handle_store[handle]
    return jsonify({
        "responseCode": 1,
        "message": f"Handle {handle} deleted"
    }), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
