from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from urllib.parse import urlsplit


class ConsumerConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    processor: str
    topic: str
    output_dir: Optional[str] = None
    max_errors: Optional[int] = None


class KafkaConfig(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    bootstrap_servers: str = Field(..., alias="bootstrap.servers")
    group_id: Optional[str] = Field(None, alias="group.id")
    auto_offset_reset: Optional[str] = Field(None, alias="auto.offset.reset")
    enable_auto_commit: Optional[bool] = Field(None, alias="enable.auto.commit")
    session_timeout_ms: Optional[int] = Field(None, alias="session.timeout.ms")

    def validate_format(self) -> list[str]:
        errors: list[str] = []

        def _valid_hostport(token: str) -> bool:
            token = token.strip()
            if not token:
                return False
            try:
                parsed = urlsplit(f"//{token}", allow_fragments=False)
                host = parsed.hostname
                port = parsed.port
                return host is not None and port is not None and 1 <= port <= 65535
            except Exception:
                return False

        invalid = [t for t in self.bootstrap_servers.split(",") if not _valid_hostport(t)]
        if invalid:
            errors.append(
                "[kafka].bootstrap.servers must be a comma-separated list of host:port; invalid: "
                + ", ".join(s.strip() for s in invalid)
            )
        return errors


class HandleConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    backend: Optional[Literal["pyhandle", "jsonl"]] = None
    server_url: Optional[str] = None
    prefix: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None


class StacConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    base_url: Optional[str] = None


class ElasticsearchConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    base_url: Optional[str] = None
    index: Optional[str] = None


class LookupConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    backend: Optional[Literal["stac", "es"]] = None
    enabled: bool = True


class SchemaConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    strict_mode: Optional[bool] = None


class PluginsCmip6Config(BaseModel):
    model_config = ConfigDict(extra="allow")
    landing_page_url: Optional[str] = None


class PluginsConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    cmip6: Optional[PluginsCmip6Config] = None


class AppConfig(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    consumer: ConsumerConfig
    kafka: KafkaConfig
    handle: Optional[HandleConfig] = None
    stac: Optional[StacConfig] = None
    elasticsearch: Optional[ElasticsearchConfig] = None
    lookup: Optional[LookupConfig] = None
    schema_config: Optional[SchemaConfig] = Field(None, alias="schema")
    plugins: Optional[PluginsConfig] = None


def validate_config(data: dict) -> tuple[list[str], list[str]]:
    """Validate config using Pydantic models and simple cross-checks.

    Returns (errors, warnings). No network calls.
    """
    errors: list[str] = []
    warnings: list[str] = []

    try:
        cfg = AppConfig.model_validate(data)
    except ValidationError as ve:
        for err in ve.errors():
            loc = ".".join(str(x) for x in err.get("loc", []))
            msg = err.get("msg", "invalid configuration")
            errors.append(f"{loc}: {msg}")
        return errors, warnings

    # kafka format checks
    errors.extend(cfg.kafka.validate_format())

    # handle backend requirements
    if cfg.handle and cfg.handle.backend not in {None, "pyhandle", "jsonl"}:
        errors.append("[handle].backend must be 'pyhandle' or 'jsonl'")
    if cfg.handle and cfg.handle.backend == "pyhandle":
        if not cfg.handle.server_url:
            errors.append("Missing required setting: [handle].server_url")
        if not cfg.handle.prefix:
            errors.append("Missing required setting: [handle].prefix")
        if (
            cfg.handle.username == "300:21.TEST/testuser"
            and cfg.handle.password == "testpass"
        ):
            warnings.append("[handle] demo credentials detected; do not use in production")

    # lookup backend requirements
    if cfg.lookup and cfg.lookup.enabled:
        if cfg.lookup.backend not in {"stac", "es"}:
            errors.append("[lookup].backend must be 'stac' or 'es' when enabled")
        elif cfg.lookup.backend == "stac":
            if not (cfg.stac and cfg.stac.base_url):
                errors.append("Missing required setting: [stac].base_url")
        elif cfg.lookup.backend == "es":
            if not (cfg.elasticsearch and cfg.elasticsearch.base_url):
                errors.append("Missing required setting: [elasticsearch].base_url")
            elif not (cfg.elasticsearch.index):
                warnings.append("[elasticsearch].index is not set; some features may be unavailable")

    # schema strict_mode type handled by Pydantic; add no-op

    # plugins cmip6 hint
    lp = cfg.plugins.cmip6.landing_page_url if (cfg.plugins and cfg.plugins.cmip6) else None
    if lp in (None, ""):
        warnings.append("[plugins.cmip6].landing_page_url not set; landing pages may be missing")

    return errors, warnings
