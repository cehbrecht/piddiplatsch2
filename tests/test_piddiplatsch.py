import json
import time
import uuid
import pytest
from click.testing import CliRunner
from piddiplatsch.cli import cli
from piddiplatsch.config import config


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


def test_send_and_consume_message(runner, example_message):
    topic = config.get("kafka", "topic")
    kafka_server = config.get("kafka", "server")

    # Write message to temp file
    with runner.isolated_filesystem():
        with open("message.json", "w") as f:
            json.dump(example_message, f)

        # Send the message
        result_send = runner.invoke(
            cli,
            [
                "send",
                "--path",
                "message.json",
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
            assert (
                value["data"]["payload"]["item"]["links"][0]["href"]
                == "http://example.com/data.nc"
            )
            process_message(key, value)
            break


def test_send_invalid_path(runner):
    result = runner.invoke(cli, ["send", "--path", "nonexistent.json"])
    assert result.exit_code == 0  # It doesn't crash
    assert "File not found" in result.output
