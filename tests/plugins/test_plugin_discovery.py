import pytest
pytestmark = [pytest.mark.plugin]

from piddiplatsch.core.registry import list_processors, get_processor
from piddiplatsch.core.processing import BaseProcessor


def test_cmip6_plugin_discovered():
    processors = list_processors()
    assert "cmip6" in processors


def test_cmip6_plugin_instantiation():
    proc = get_processor("cmip6", dry_run=True)
    assert isinstance(proc, BaseProcessor)
