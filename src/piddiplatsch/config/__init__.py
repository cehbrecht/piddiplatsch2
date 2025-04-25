import logging
import os
from pathlib import Path
from typing import Optional

import toml
from rich.logging import RichHandler

DEFAULT_CONFIG_PATH = Path(__file__).parent / "default_config.toml"


class Config:
    def __init__(self):
        self.config_data = self._load_toml(DEFAULT_CONFIG_PATH)

    def _load_toml(self, path: Path):
        if path.exists():
            return toml.load(path)
        return {}

    def load_user_config(self, user_config_path: Optional[str]):
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

    def get(self, section: str, key: str, fallback=None):
        return self.config_data.get(section, {}).get(key, fallback)

    def configure_logging(self, debug: bool = False, logfile: Optional[str] = None):
        log_level = logging.DEBUG if debug else logging.INFO

        handlers = []
        if logfile:
            handlers.append(logging.FileHandler(logfile))
        else:
            handlers.append(RichHandler(rich_tracebacks=True))

        logging.basicConfig(
            level=log_level,
            format=(
                "%(message)s"
                if not logfile
                else "%(asctime)s - %(levelname)s - %(message)s"
            ),
            datefmt="[%X]",
            handlers=handlers,
        )


# singleton instance
config = Config()
