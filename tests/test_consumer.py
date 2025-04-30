import pytest
import logging
from unittest.mock import patch, MagicMock

from piddiplatsch.consumer import Consumer


@pytest.fixture
def mock_handle_client():
    with patch("piddiplatsch.consumer.HandleClient") as mock_cls:
        instance = MagicMock()
        mock_cls.return_value = instance
        yield instance


@pytest.fixture
def sample_message():
    return {
        "data": {
            "payload": {
                "item": {
                    "id": "hdl:123/foo",
                    "links": [{"href": "http://example.com/item.nc"}],
                }
            }
        }
    }


def test_process_message_with_valid_message(mock_handle_client, sample_message):
    consumer = Consumer(topic="test", kafka_server="localhost:9092")
    key = "test-key"

    # Run
    consumer.process_message(key, sample_message)

    # Check PID and record built
    expected_pid = "hdl:123/foo"
    expected_record = {
        "URL": "http://example.com/item.nc",
        "CHECKSUM": None,
    }

    mock_handle_client.add_item.assert_called_once_with(expected_pid, expected_record)


def test_process_message_with_missing_id_and_url(mock_handle_client):
    consumer = Consumer(topic="test", kafka_server="localhost:9092")
    key = "another-key"

    broken_message = {"data": {"payload": {"item": {"links": []}}}}  # No href

    consumer.process_message(key, broken_message)

    args, _ = mock_handle_client.add_item.call_args
    pid, record = args

    assert pid is not None  # Should fall back to UUID
    assert record == {"URL": None, "CHECKSUM": None}


def test_consume_decodes_valid_json_message(monkeypatch):
    consumer = Consumer(topic="test", kafka_server="localhost:9092")

    class FakeMessage:
        def key(self):
            return b"test"

        def value(self):
            return b'{"foo": "bar"}'

        def error(self):
            return None

    fake_kafka_consumer = MagicMock()
    fake_kafka_consumer.poll.side_effect = [FakeMessage(), None, KeyboardInterrupt]
    consumer.consumer = fake_kafka_consumer

    messages = []
    try:
        for key, val in consumer.consume():
            messages.append((key, val))
    except KeyboardInterrupt:
        pass

    assert messages == [("test", {"foo": "bar"})]
