import pytest
import requests
import json

HANDLE_SERVER_URL = "http://localhost:5000/handles"
HANDLE_PREFIX = "21.T11148"

def test_register_cmip7_dataset():
    dataset_id = "cmip7.test.dataset.001"
    pid = f"{HANDLE_PREFIX}/{dataset_id}"
    
    data = {"title": "Test CMIP7 Dataset", "version": "1.0"}
    response = requests.put(f"{HANDLE_SERVER_URL}/{pid}", json=data)
    
    assert response.status_code == 201
    assert response.json()["message"] == "Handle created"

    # Verify registration
    response = requests.get(f"{HANDLE_SERVER_URL}/{pid}")
    assert response.status_code == 200
    assert response.json()["title"] == "Test CMIP7 Dataset"

def test_register_netcdf_file():
    tracking_id = "abc-123-xyz"
    pid = f"{HANDLE_PREFIX}/{tracking_id}"
    
    data = {"file": "test_file.nc", "checksum": "abcdef123456"}
    response = requests.put(f"{HANDLE_SERVER_URL}/{pid}", json=data)
    
    assert response.status_code == 201
    assert response.json()["message"] == "Handle created"

    # Verify registration
    response = requests.get(f"{HANDLE_SERVER_URL}/{pid}")
    assert response.status_code == 200
    assert response.json()["file"] == "test_file.nc"
    assert response.json()["checksum"] == "abcdef123456"
