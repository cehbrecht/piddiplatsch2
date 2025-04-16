from flask import Flask, request, jsonify, make_response
from functools import wraps
import base64

app = Flask(__name__)
mock_db = {}

USERNAME = "300:yourusername"
PASSWORD = "yourpassword"


# --- Auth check ---
def check_auth(auth_header):
    if not auth_header:
        return False
    try:
        auth_type, creds = auth_header.split()
        if auth_type.lower() != "basic":
            return False
        decoded = base64.b64decode(creds).decode()
        user, pwd = decoded.split(":", 1)
        return user == USERNAME and pwd == PASSWORD
    except Exception:
        return False


def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not check_auth(request.headers.get("Authorization")):
            return make_response(
                "Unauthorized",
                401,
                {"WWW-Authenticate": 'Basic realm="Login Required"'},
            )
        return f(*args, **kwargs)

    return decorated


@app.before_request
def log_headers():
    print("--- Incoming request ---")
    print(request.method, request.path)
    print("Headers:")
    for k, v in request.headers.items():
        print(f"{k}: {v}")
    print("Body:", request.get_data().decode())


# --- Handle Routes ---
@app.route("/api/handles/foo/bar", methods=["GET"])
def dummy_user_handle():
    return (
        jsonify(
            {
                "responseCode": 1,
                "handle": "foo/bar",
                "values": [
                    {
                        "index": 1,
                        "type": "HS_ADMIN",
                        "data": {
                            "format": "admin",
                            "value": {
                                "handle": "foo/bar",
                                "index": 300,
                                "permissions": "111111111111",
                            },
                        },
                    }
                ],
            }
        ),
        200,
    )


@app.route("/api/handles/foo/bar", methods=["PUT"])
def register_handle():
    data = request.get_json()
    # You could log or inspect the data to verify it's being passed correctly
    print("Registration Data:", data)
    return jsonify({"responseCode": 1, "handle": "foo/bar", "values": data}), 200


@app.route("/api/handles/<path:handle>", methods=["PUT"])
@requires_auth
def create_or_update_handle(handle):
    data = request.get_json()
    mock_db[handle] = data.get("values", [])
    return jsonify({"responseCode": 1, "handle": handle}), 201


@app.route("/api/handles/<path:handle>", methods=["GET"])
def resolve_handle(handle):
    values = mock_db.get(handle)
    if not values:
        return jsonify({"responseCode": 100}), 404
    return jsonify({"responseCode": 1, "handle": handle, "values": values}), 200


@app.route("/api/handles/<path:handle>", methods=["DELETE"])
@requires_auth
def delete_handle(handle):
    if handle in mock_db:
        del mock_db[handle]
        return jsonify({"responseCode": 1, "handle": handle}), 200
    else:
        return jsonify({"responseCode": 100}), 404


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


if __name__ == "__main__":
    app.run(debug=True)
