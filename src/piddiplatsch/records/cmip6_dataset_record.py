import logging
from functools import cached_property
from typing import Any

from piddiplatsch.config import config
from piddiplatsch.models import CMIP6DatasetModel, HostingNode
from piddiplatsch.records.base import BaseCMIP6Record
from piddiplatsch.records.utils import parse_datetime, parse_pid
from piddiplatsch.utils.pid import asset_pid, build_handle, item_pid


class CMIP6DatasetRecord(BaseCMIP6Record):
    """Wraps a validated CMIP6 STAC item and prepares Handle records."""

    def __init__(
        self,
        item: dict[str, Any],
        strict: bool,
        exclude_keys: list[str] | None = None,
        additional_attributes: dict[str, Any] | None = None,
    ):
        super().__init__(item, additional_attributes, strict=strict)
        self.exclude_keys = set(exclude_keys or [])
        self.max_parts = config.get("cmip6", {}).get("max_parts", -1)

    @cached_property
    def pid(self) -> str:
        pid_ = parse_pid(self.get_property("pid"))
        if not pid_:
            pid_ = item_pid(self.item_id)
            logging.warning(
                f"Creating new dataset pid: pid={pid_}, ds_id={self.item_id}"
            )
        else:
            logging.warning(
                f"Using existing dataset pid: pid={pid_}, ds_id={self.item_id}"
            )
        return pid_

    @cached_property
    def dataset_id(self) -> str:
        parts = self.item_id.rsplit(".", 1)
        if len(parts) < 2:
            logging.warning(f"Unable to parse dataset ID from: {self.item_id}")
            return self.item_id
        return parts[0]

    @cached_property
    def dataset_version(self) -> str:
        parts = self.item_id.rsplit(".", 1)
        if len(parts) < 2:
            logging.warning(f"No version found in ID: {self.item_id}")
            return ""
        return parts[1]

    @cached_property
    def has_parts(self) -> list[str]:
        parts = []
        for key in self.assets.keys():
            if key in self.exclude_keys:
                continue
            if self.max_parts > -1 and len(parts) >= self.max_parts:
                logging.debug(f"Reached limit of {self.max_parts} assets.")
                break
            pid = asset_pid(self.item_id, key)
            handle = build_handle(pid, as_uri=True)
            parts.append(handle)
        return parts

    @cached_property
    def is_part_of(self) -> str | None:
        # Placeholder - can be updated to extract actual parent PID if needed
        return None

    @cached_property
    def host(self) -> str:
        host = None
        for key in ("reference_file", "data0000", "data0001"):
            host = self.get_asset_property(key, "alternate:name")
            if host:
                break

        host = host or "unknown"

        return host

    @cached_property
    def published_on(self) -> str:
        published_on = None
        for key in ("reference_file", "data0000", "data0001"):
            published_on = self.get_asset_property(key, "published_on")
            if published_on:
                break

        if not published_on:
            published_on = self.default_publication_time

        return parse_datetime(published_on)

    @cached_property
    def hosting_node(self) -> HostingNode:
        return HostingNode(host=self.host, published_on=self.published_on)

    @cached_property
    def replica_nodes(self) -> list[HostingNode]:
        nodes = []
        known_hosts = []
        for key in ("reference_file", "data0000", "data0001"):
            alternates = self.get_asset_property(key, "alternate", {})
            for host, values in alternates.items():
                published_on = values.get("published_on")
                if not published_on:
                    published_on = self.published_on
                if host not in known_hosts:
                    known_hosts.append(host)
                    nodes.append(HostingNode(host=host, published_on=published_on))
        return nodes

    @cached_property
    def retracted(self) -> bool:
        retracted_ = self.item.get("properties", {}).get("retracted", "false")
        retracted_ = bool(retracted_)
        return retracted_

    @cached_property
    def previous_version(self) -> str:
        previous = None
        return previous

    def as_handle_model(self) -> CMIP6DatasetModel:
        dsm = CMIP6DatasetModel(
            URL=self.url,
            DATASET_ID=self.dataset_id,
            DATASET_VERSION=self.dataset_version,
            PREVIOUS_VERSION=self.previous_version,
            HAS_PARTS=self.has_parts,
            IS_PART_OF=self.is_part_of,
            HOSTING_NODE=self.hosting_node,
            REPLICA_NODES=self.replica_nodes,
            RETRACTED=self.retracted,
        )

        if self.retracted:
            logging.warning(f"Dataset with id={self.dataset_id} was retracted!")

        if self.replica_nodes:
            logging.info(
                f"Dataset with id={self.dataset_id} has {len(self.replica_nodes)} replica!"
            )

        dsm.set_pid(self.pid)
        return dsm
