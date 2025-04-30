import pytest
from unittest.mock import patch, MagicMock

from piddiplatsch.client import (
    get_producer,
    get_admin_client,
    ensure_topic_exists,
    send_message,
)


@pytest.fixture
def kafka_config():
    return "localhost:9092", "test-topic"


def test_get_producer_returns_instance():
    with patch("piddiplatsch.client.Producer") as mock_producer:
        get_producer("localhost:9092")
        mock_producer.assert_called_once_with({"bootstrap.servers": "localhost:9092"})


def test_get_admin_client_returns_instance():
    with patch("piddiplatsch.client.AdminClient") as mock_admin:
        get_admin_client("localhost:9092")
        mock_admin.assert_called_once_with({"bootstrap.servers": "localhost:9092"})


def test_ensure_topic_exists_creates_missing_topic(kafka_config):
    kafka_server, topic = kafka_config

    mock_admin = MagicMock()
    mock_admin.list_topics.return_value.topics = {}
    mock_future = MagicMock()
    mock_future.result.return_value = None
    mock_admin.create_topics.return_value = {topic: mock_future}

    with patch("piddiplatsch.client.get_admin_client", return_value=mock_admin):
        ensure_topic_exists(kafka_server, topic)

    mock_admin.create_topics.assert_called_once()
