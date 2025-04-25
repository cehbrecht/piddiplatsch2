import pytest
from piddiplatsch.testing.mock_handle_server import app, HANDLE_PREFIX, _reset_handles

TEST_SUFFIX = "pytest-test"
TEST_HANDLE = f"{HANDLE_PREFIX}/{TEST_SUFFIX}"


@pytest.fixture(autouse=True)
def client():
    # Reset the in-memory state before each test
    _reset_handles()
    with app.test_client() as client:
        yield client


def test_get_nonexistent_handle(client):
    response = client.get(f"/api/handles/{HANDLE_PREFIX}/nonexistent")
    assert response.status_code == 200
    data = response.get_json()
    assert data["responseCode"] == 100


def test_put_handle_without_overwrite(client):
    record = {
        "values": [
            {"index": 1, "type": "URL", "data": {"value": "https://example.com"}}
        ]
    }

    res1 = client.put(f"/api/handles/{HANDLE_PREFIX}/{TEST_SUFFIX}", json=record)
    assert res1.status_code == 200

    res2 = client.put(f"/api/handles/{HANDLE_PREFIX}/{TEST_SUFFIX}", json=record)
    assert res2.status_code == 409


def test_put_handle_with_overwrite(client):
    record = {
        "values": [
            {"index": 1, "type": "URL", "data": {"value": "https://new.example.com"}}
        ]
    }

    res = client.put(
        f"/api/handles/{HANDLE_PREFIX}/{TEST_SUFFIX}?overwrite=true", json=record
    )
    assert res.status_code == 200
    assert "registered" in res.get_json()["message"]


def test_get_existing_handle(client):
    record = {
        "values": [
            {"index": 1, "type": "URL", "data": {"value": "https://example.com"}}
        ]
    }

    client.put(f"/api/handles/{HANDLE_PREFIX}/{TEST_SUFFIX}", json=record)
    res = client.get(f"/api/handles/{HANDLE_PREFIX}/{TEST_SUFFIX}")
    data = res.get_json()
    assert data["handle"] == TEST_HANDLE
    assert data["responseCode"] == 1
