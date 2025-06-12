import pluggy
import importlib.metadata
from piddiplatsch.plugin import MessageProcessorSpec

def load_single_plugin(processor_name: str):
    pm = pluggy.PluginManager("piddiplatsch")
    pm.add_hookspecs(MessageProcessorSpec)

    # Discover all available entry points
    entry_points = importlib.metadata.entry_points(group="piddiplatsch")
    entry_point_map = {ep.name: ep for ep in entry_points}

    if processor_name not in entry_point_map:
        raise ValueError(f"Processor plugin '{processor_name}' not found among: {list(entry_point_map)}")

    plugin = entry_point_map[processor_name].load()()
    pm.register(plugin)

    # Assert that exactly one plugin is active
    plugins = pm.get_plugins()
    if len(plugins) != 1:
        raise RuntimeError(f"Expected one plugin, found: {len(plugins)}")

    return plugin
