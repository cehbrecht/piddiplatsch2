import pytest

from piddiplatsch.consumer import ConsumerPipeline, DirectConsumer
from piddiplatsch.handles.jsonl_backend import JsonlHandleBackend
from piddiplatsch.plugin_loader import load_single_plugin

pytestmark = pytest.mark.integration


def test_plugin_uses_jsonl_backend_in_dry_run():
    plugin = load_single_plugin("cmip6", dry_run=True)
    assert isinstance(
        getattr(plugin.handle_backend, "backend", None), JsonlHandleBackend
    )


def test_pipeline_initializes_jsonl_backend_in_dry_run():
    consumer = DirectConsumer(messages=[])
    pipeline = ConsumerPipeline(consumer, processor="cmip6", dry_run=True)
    assert isinstance(
        getattr(pipeline.processor.handle_backend, "backend", None), JsonlHandleBackend
    )
