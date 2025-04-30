import pytest
from unittest import mock
from piddiplatsch import consumer


def test_start_consumer_invokes_plugin_processing():
    # Arrange
    fake_topic = "test-topic"
    fake_server = "localhost:9092"
    mock_consumer_instance = mock.MagicMock()
    mock_consumer_instance.consume.return_value = [("key1", {"foo": "bar"})]

    mock_handle_client = mock.Mock()

    mock_plugin_instance = mock.Mock()
    mock_plugin_instance.can_process.return_value = True

    with (
        mock.patch(
            "piddiplatsch.consumer.Consumer", return_value=mock_consumer_instance
        ),
        mock.patch(
            "piddiplatsch.consumer.load_processor",
            return_value=lambda: mock_plugin_instance,
        ),
    ):
        # Act
        consumer.start_consumer(fake_topic, fake_server)

        # Assert
        mock_consumer_instance.consume.assert_called()
        mock_plugin_instance.can_process.assert_called_once_with("key1", {"foo": "bar"})
        mock_plugin_instance.process.assert_called_once_with(
            "key1", {"foo": "bar"}, mock_handle_client
        )


def test_start_consumer_skips_unmatched_plugin():
    # Arrange
    fake_topic = "test-topic"
    fake_server = "localhost:9092"
    mock_consumer_instance = mock.MagicMock()
    mock_consumer_instance.consume.return_value = [("key2", {"baz": "qux"})]

    mock_handle_client = mock.Mock()

    mock_plugin_instance = mock.Mock()
    mock_plugin_instance.can_process.return_value = False  # Should skip processing

    with (
        mock.patch(
            "piddiplatsch.consumer.Consumer", return_value=mock_consumer_instance
        ),
        mock.patch(
            "piddiplatsch.consumer.load_processor",
            return_value=lambda: mock_plugin_instance,
        ),
    ):
        # Act
        consumer.start_consumer(fake_topic, fake_server)

        # Assert
        mock_plugin_instance.can_process.assert_called_once_with("key2", {"baz": "qux"})
        mock_plugin_instance.process.assert_not_called()
