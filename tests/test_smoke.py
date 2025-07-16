import json
import uuid
import os
import requests
import pytest
from piddiplatsch.cli import cli


@pytest.fixture
def example_message():
    return {
        "data": {
            "payload": {
                "item": {
                    "id": f"cmip6-{uuid.uuid4()}",
                    "properties": {"version": "v20250430"},
                    "assets": {"reference_file": {"alternate:name": "example.com"}},
                    "links": [{"href": "http://example.com/data.nc"}],
                }
            }
        }
    }


def send_and_consume_message(runner, message):
    # Send the message
    result_send = runner.invoke(
        cli,
        [
            "send",
            "--message",
            json.dumps(message),  # ensure it's a string
        ],
    )

    assert result_send.exit_code == 0
    assert "📤 Message delivered" in result_send.output


@pytest.mark.online
def test_send_valid_example_message(runner, example_message):

    send_and_consume_message(runner, example_message)


@pytest.mark.online
def test_send_valid_cmip6_mpi(runner, testdata_path):
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

    send_and_consume_message(runner, message)
