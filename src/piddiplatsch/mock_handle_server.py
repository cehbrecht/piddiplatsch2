from flask import Flask, request, jsonify

app = Flask(__name__)
mock_db = {}

@app.route("/handles/<handle>", methods=["GET"])
def get_handle(handle):
    if handle in mock_db:
        return jsonify(mock_db[handle]), 200
    return jsonify({"error": "Handle not found"}), 404

@app.route("/handles/<handle>", methods=["PUT"])
def create_handle(handle):
    mock_db[handle] = request.json
    return jsonify({"message": "Handle created"}), 201

@app.route("/handles/<handle>", methods=["POST"])
def update_handle(handle):
    if handle in mock_db:
        mock_db[handle].update(request.json)
        return jsonify({"message": "Handle updated"}), 200
    return jsonify({"error": "Handle not found"}), 404

@app.route("/handles/<handle>", methods=["DELETE"])
def delete_handle(handle):
    if handle in mock_db:
        del mock_db[handle]
        return jsonify({"message": "Handle deleted"}), 200
    return jsonify({"error": "Handle not found"}), 404

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
