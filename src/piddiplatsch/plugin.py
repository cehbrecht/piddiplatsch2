import pluggy

# Initialize hook markers for piddiplatsch
hookspec = pluggy.HookspecMarker("piddiplatsch")
hookimpl = pluggy.HookimplMarker("piddiplatsch")


class MessageProcessorSpec:
    @hookspec
    def can_process(self, key, value) -> bool:
        """Return True if this processor can handle the message."""
        pass

    @hookspec
    def process(self, key, value, handle_client) -> None:
        """Process the message."""
        pass


# A global plugin manager for loading and managing plugins
pm = pluggy.PluginManager("piddiplatsch")
pm.add_hookspecs(MessageProcessorSpec)


def load_plugin(plugin_class):
    """Load a plugin by class name and return the instance."""
    plugin = pm.get_plugin(plugin_class)
    if plugin:
        return plugin()
    return None
