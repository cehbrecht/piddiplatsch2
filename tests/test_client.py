from unittest.mock import MagicMock, patch

import pytest

from piddiplatsch.testing.kafka_client import ensure_topic_exists


@pytest.fixture
def topic():
    return "CMIP6-test"


@pytest.fixture
def kafka_cfg():
    return {
        "bootstrap.servers": "localhost:9092",
        "group.id": "test-topic",
    }


def test_ensure_topic_exists_creates_missing_topic(topic, kafka_cfg):
    mock_admin = MagicMock()
    mock_admin.list_topics.return_value.topics = {}
    mock_future = MagicMock()
    mock_future.result.return_value = None
    mock_admin.create_topics.return_value = {topic: mock_future}

    with patch(
        "piddiplatsch.testing.kafka_client.get_admin_client", return_value=mock_admin
    ):
        ensure_topic_exists(topic, kafka_cfg)

    mock_admin.create_topics.assert_called_once()
