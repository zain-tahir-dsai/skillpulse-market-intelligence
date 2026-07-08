from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
SOURCE_CONFIG_PATH = Path("config/sources.yaml")


@dataclass(frozen=True)
class AdzunaSourceConfig:
    enabled: bool
    base_url: str
    default_country: str
    default_page_size: int
    max_pages_per_run: int


@dataclass(frozen=True)
class RemoteOkSourceConfig:
    enabled: bool
    base_url: str


@dataclass(frozen=True)
class SourceConfig:
    adzuna: AdzunaSourceConfig
    remoteok: RemoteOkSourceConfig


def _require_mapping(
    value: Any,
    field_name: str,
) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"'{field_name}' must be a YAML mapping.")

    return value


def load_source_config(
    config_path: str | Path = "config/sources.yaml",
) -> SourceConfig:
    """Load and validate source configuration from YAML."""
    path = Path(config_path)

    if not path.exists():
        raise FileNotFoundError(
            f"Source configuration file not found: {path}"
        )

    with SOURCE_CONFIG_PATH.open("r", encoding="utf-8") as source_file:
        raw_config = yaml.safe_load(source_file) or {}

    root = _require_mapping(raw_config, "root")
    adzuna_raw = _require_mapping(root.get("adzuna"), "adzuna")
    remoteok_raw = _require_mapping(root.get("remoteok"), "remoteok")

    try:
        return SourceConfig(
            adzuna=AdzunaSourceConfig(
                enabled=bool(adzuna_raw["enabled"]),
                base_url=str(adzuna_raw["base_url"]),
                default_country=str(adzuna_raw["default_country"]),
                default_page_size=int(adzuna_raw["default_page_size"]),
                max_pages_per_run=int(adzuna_raw["max_pages_per_run"]),
            ),
            remoteok=RemoteOkSourceConfig(
                enabled=bool(remoteok_raw["enabled"]),
                base_url=str(remoteok_raw["base_url"]),
            ),
        )
    except KeyError as error:
        raise ValueError(
            f"Missing required source configuration field: {error.args[0]}"
        ) from error
