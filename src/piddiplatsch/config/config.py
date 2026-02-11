import logging
from pathlib import Path

import toml
from rich.logging import RichHandler

DEFAULT_CONFIG_PATH = Path(__file__).parent / "default.toml"


class Config:
    def __init__(self):
        self.config_data = self._load_toml(DEFAULT_CONFIG_PATH)

    def _load_toml(self, path: Path):
        if path.exists():
            return toml.load(path)
        return {}

    # --- Test / internal helper to override config values ---
    def _set(self, section: str, key: str | None, value):
        """
        Private setter for testing or dynamic overrides.
        Example:
            config._set("cmip6", "max_parts", 5)
            config._set("schema", None, {"strict_mode": True})
        """
        if key is None:
            # Replace the entire section
            self.config_data[section] = value
        else:
            if section not in self.config_data:
                self.config_data[section] = {}
            self.config_data[section][key] = value

    def load_user_config(self, user_config_path: str | None):
        if user_config_path:
            user_path = Path(user_config_path)
            if user_path.exists():
                user_data = self._load_toml(user_path)
                self._merge_dicts(self.config_data, user_data)

    def _merge_dicts(self, base, override):
        for key, value in override.items():
            if isinstance(value, dict) and key in base:
                self._merge_dicts(base[key], value)
            else:
                base[key] = value

    def get(self, section: str, key: str | None = None, fallback=None):
        cfg = self.config_data.get(section, {})
        if key:
            value = cfg.get(key, fallback)
        else:
            value = cfg
        return value

    def get_plugin(self, name: str, key: str | None = None, fallback=None):
        """Return plugin-scoped config with backwards compatibility.

        Merges `[plugins.<name>]` with legacy top-level `[<name>]`,
        giving precedence to the legacy top-level for test overrides.
        """
        plugins = self.config_data.get("plugins", {})
        merged: dict = dict(plugins.get(name, {}) or {})
        legacy = self.config_data.get(name, {}) or {}
        # Let legacy (tests/user overrides) take precedence over defaults
        merged.update(legacy)
        if key:
            return merged.get(key, fallback)
        return merged

    def configure_logging(self, debug: bool = False, log: str | None = None):
        log_level = logging.DEBUG if debug else logging.INFO

        handlers = []

        if not log:
            console = True
        else:
            console = False

        if console:
            handlers.append(RichHandler(rich_tracebacks=True))
        else:
            handlers.append(logging.FileHandler(log))

        logging.basicConfig(
            level=log_level,
            format=(
                "%(message)s"
                if console
                else "%(asctime)s - %(levelname)s - %(message)s"
            ),
            datefmt="[%X]",
            handlers=handlers,
        )

    def validate(self) -> tuple[list[str], list[str]]:
        """Validate the loaded configuration.

        Returns (errors, warnings).
        The function performs structural and semantic checks only; no network calls.
        """
        errors: list[str] = []
        warnings: list[str] = []

        def require(section: str, key: str):
            value = self.get(section, key)
            if value is None or (isinstance(value, str) and value.strip() == ""):
                errors.append(f"Missing required setting: [{section}].{key}")
            return value

        # consumer basics
        processor = require("consumer", "processor")
        topic = require("consumer", "topic")
        if processor and not isinstance(processor, str):
            errors.append("[consumer].processor must be a string")
        if topic and not isinstance(topic, str):
            errors.append("[consumer].topic must be a string")

        # kafka config
        bs = require("kafka", "bootstrap.servers")
        if bs and not isinstance(bs, str):
            errors.append("[kafka].bootstrap.servers must be a string host:port list")

        # handle backend
        backend = self.get("handle", "backend")
        if backend not in {"pyhandle", "jsonl", None}:
            errors.append("[handle].backend must be 'pyhandle' or 'jsonl'")
        if backend == "pyhandle":
            require("handle", "server_url")
            require("handle", "prefix")
            # Warn if default demo credentials are present
            user = self.get("handle", "username")
            pwd = self.get("handle", "password")
            if user == "300:21.TEST/testuser" and pwd == "testpass":
                warnings.append("[handle] demo credentials detected; do not use in production")

        # lookup backend
        enabled = self.get("lookup", "enabled", True)
        backend_lookup = self.get("lookup", "backend")
        if enabled:
            if backend_lookup not in {"stac", "es"}:
                errors.append("[lookup].backend must be 'stac' or 'es' when enabled")
            if backend_lookup == "stac":
                require("stac", "base_url")
            if backend_lookup == "es":
                require("elasticsearch", "base_url")
                # index often required; warn if missing
                if self.get("elasticsearch", "index") in (None, ""):
                    warnings.append("[elasticsearch].index is not set; some features may be unavailable")

        # schema
        strict = self.get("schema", "strict_mode")
        if strict is not None and not isinstance(strict, bool):
            errors.append("[schema].strict_mode must be a boolean")

        # plugins: cmip6 hints
        cmip6_lp = self.get_plugin("cmip6", "landing_page_url")
        if cmip6_lp in (None, ""):
            warnings.append("[plugins.cmip6].landing_page_url not set; landing pages may be missing")

        return errors, warnings


# singleton instance
config = Config()
