import json
import time
import uuid
import os
import pytest
from piddiplatsch.cli import cli
from piddiplatsch.config import config


@pytest.fixture(scope="module")
def kafka_settings():
    return {
        "topic": config.get("kafka", "topic"),
        "kafka_server": config.get("kafka", "server"),
    }


@pytest.fixture
def example_message():
    return {
        "data": {
            "payload": {
                "item": {
                    "id": f"cmip7-{uuid.uuid4()}",
                    "links": [{"href": "http://example.com/data.nc"}],
                }
            }
        }
    }


import requests


def send_and_consume_message(runner, message, topic, kafka_server):
    # Get location
    location = message["data"]["payload"]["item"]["links"][0]["href"]
    pid = message["data"]["payload"]["item"]["id"]

    # Send the message
    result_send = runner.invoke(
        cli,
        [
            "send",
            "--message",
            json.dumps(message),  # ensure it's a string
            "--topic",
            topic,
            "--kafka-server",
            kafka_server,
        ],
    )
    assert result_send.exit_code == 0
    assert "📤 Message delivered" in result_send.output

    time.sleep(2)  # Allow Kafka to propagate

    from piddiplatsch.consumer import Consumer, build_client, load_processor

    consumer = Consumer(topic, kafka_server)
    handle_client = build_client()
    processor = load_processor()

    for key, value in consumer.consume():
        assert key
        assert value["data"]["payload"]["item"]["links"][0]["href"] == location
        processor.process(key, value, handle_client)
        break

    # Verify the handle was registered in the mock handle server
    mock_handle_url = config.get("handle", "server_url").rstrip("/")
    handle_prefix = config.get("handle", "prefix")
    full_handle = f"{handle_prefix}/{pid}"

    response = requests.get(f"{mock_handle_url}/api/handles/{full_handle}")
    assert response.status_code == 200, f"Handle not found: {full_handle}"
    assert response.json()["handle"] == full_handle


@pytest.mark.online
def test_send_valid_cmip6_mpi(runner, testdata_path, kafka_settings):
    # Path to the JSON file in tests/testdata
    json_path = os.path.join(
        testdata_path,
        "CMIP6",
        "CMIP6.ScenarioMIP.MPI-M.MPI-ESM1-2-LR.ssp126.r1i1p1f1.day.tasmin.gn.v20190710.json",
    )

    # Ensure the file exists before continuing
    assert os.path.exists(json_path), f"Missing file: {json_path}"

    with open(json_path, "r") as file:
        message = json.load(file)

    send_and_consume_message(
        runner, message, kafka_settings["topic"], kafka_settings["kafka_server"]
    )
