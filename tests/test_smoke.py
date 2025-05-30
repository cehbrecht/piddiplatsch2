import json
import time
import uuid
import os
import requests
import pytest
from piddiplatsch.cli import cli
from piddiplatsch.config import config
from piddiplatsch.consumer import ConsumerPipeline


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
                    "properties": {"version": "v20250430"},
                    "assets": {"reference_file": {"alternate:name": "example.com"}},
                    "links": [{"href": "http://example.com/data.nc"}],
                }
            }
        }
    }


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

    pipeline = ConsumerPipeline(topic, kafka_server)

    key, value = next(pipeline.consumer.consume())
    assert key
    assert value
    pipeline.process_message(key, value)

    # Verify the handle was registered in the mock handle server
    mock_handle_url = config.get("handle", "server_url").rstrip("/")
    handle_prefix = config.get("handle", "prefix")
    full_handle = f"{handle_prefix}/{pid}"

    response = requests.get(f"{mock_handle_url}/api/handles/{full_handle}")
    assert response.status_code == 200, f"Handle not found: {full_handle}"
    data = response.json()
    assert data["handle"] == full_handle
    # unpack handle values
    values = {}
    for value in data["values"]:
        values[value["type"]] = value["data"]
    assert values["URL"] == location


@pytest.mark.online
def test_send_valid_example_message(runner, example_message, kafka_settings):

    send_and_consume_message(
        runner, example_message, kafka_settings["topic"], kafka_settings["kafka_server"]
    )


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
