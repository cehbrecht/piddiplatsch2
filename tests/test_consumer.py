import pytest
from unittest import mock
from piddiplatsch import consumer


def test_start_consumer_invokes_consume_loop():
    # Arrange
    fake_topic = "test-topic"
    fake_server = "localhost:9092"
    mock_consumer_instance = mock.MagicMock()
    mock_consumer_instance.consume.return_value = [("key1", {"foo": "bar"})]

    with (
        mock.patch(
            "piddiplatsch.consumer.Consumer", return_value=mock_consumer_instance
        ),
        mock.patch("piddiplatsch.consumer.process_message") as mock_process,
    ):

        # Act
        consumer.start_consumer(fake_topic, fake_server)

        # Assert
        mock_consumer_instance.consume.assert_called()
        mock_process.assert_called_once_with("key1", {"foo": "bar"})


def test_process_message_calls_add_item():
    # Arrange
    key = "test-key"
    value = {
        "data": {
            "payload": {
                "item": {"id": "1234", "links": [{"href": "http://example.com"}]}
            }
        }
    }

    with mock.patch("piddiplatsch.consumer.build_client") as mock_client_factory:
        mock_client = mock.Mock()
        mock_client_factory.return_value = mock_client

        # Act
        consumer.process_message(key, value)

        # Assert
        mock_client.add_item.assert_called_once_with(
            "1234", {"URL": "http://example.com", "CHECKSUM": None}
        )
