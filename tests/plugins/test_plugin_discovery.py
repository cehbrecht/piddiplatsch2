import pytest

from piddiplatsch.core.processing import BaseProcessor
from piddiplatsch.core.registry import get_processor, list_processors

pytestmark = [pytest.mark.plugin]


def test_cmip6_plugin_discovered():
    processors = list_processors()
    assert "cmip6" in processors


def test_cmip6_plugin_instantiation():
    proc = get_processor("cmip6", dry_run=True)
    assert isinstance(proc, BaseProcessor)
