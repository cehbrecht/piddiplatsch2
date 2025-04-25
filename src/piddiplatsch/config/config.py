import logging
import os
from pathlib import Path
from typing import Optional

import toml
from rich.logging import RichHandler

DEFAULT_CONFIG_PATH = Path(__file__).parent / "default_config.toml"

class Config:
    def __init__(self):
        self.config_data = self._load_default_config()

    def _load_default_config(self):
        if DEFAULT_CONFIG_PATH.exists():
            return toml.load(DEFAULT_CONFIG_PATH)
        return {}

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
            format="%(message)s" if not logfile else "%(asctime)s - %(levelname)s - %(message)s",
            datefmt="[%X]",
            handlers=handlers,
        )

config = Config()
