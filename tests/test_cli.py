import json
import time
import uuid
import os
import pytest
from click.testing import CliRunner
from piddiplatsch.cli import cli
from piddiplatsch.config import config


@pytest.fixture(scope="module")
def kafka_settings():
    return {
        "topic": config.get("kafka", "topic"),
        "kafka_server": config.get("kafka", "server"),
    }


@pytest.fixture
def runner():
    return CliRunner()


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


def send_and_consume_message(runner, message, topic, kafka_server):
    # Get location
    location = message["data"]["payload"]["item"]["links"][0]["href"]

    # Send the message
    result_send = runner.invoke(
        cli,
        [
            "send",
            "--message",
            message,
            "--topic",
            topic,
            "--kafka-server",
            kafka_server,
        ],
    )
    assert result_send.exit_code == 0
    assert "📤 Message delivered" in result_send.output

    # Allow Kafka to propagate message
    time.sleep(2)

    # Start the consumer (consume only one message and exit)
    from piddiplatsch.consumer import Consumer, process_message

    consumer = Consumer(topic, kafka_server)

    # Grab one message from the topic
    for key, value in consumer.consume():
        assert key
        assert value["data"]["payload"]["item"]["links"][0]["href"] == location
        process_message(key, value)
        break


@pytest.mark.online
def test_send_and_consume_message(runner, example_message, kafka_settings):
    send_and_consume_message(
        runner,
        example_message,
        kafka_settings["topic"],
        kafka_settings["kafka_server"],
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


def test_send_invalid_path(runner):
    result = runner.invoke(cli, ["send", "--path", "nonexistent.json"])
    assert result.exit_code == 0  # It doesn't crash
    assert "File not found" in result.output
