import pytest

from config.source_config import (
    AdzunaSourceConfig,
    RemoteOkSourceConfig,
    SourceConfig,
    load_source_config,
)
from src.ingestion.sources.remoteok import RemoteOkIngestor


def test_load_source_config_reads_valid_yaml() -> None:
    config = load_source_config()

    assert config.remoteok.enabled is True
    assert config.remoteok.base_url == "https://remoteok.com/api"

    assert config.adzuna.enabled is True
    assert config.adzuna.base_url == "https://api.adzuna.com/v1/api/jobs"
    assert config.adzuna.default_country == "gb"
    assert config.adzuna.default_page_size == 50
    assert config.adzuna.max_pages_per_run == 5


def test_load_source_config_raises_for_missing_required_field(
    tmp_path,
    monkeypatch,
) -> None:
    invalid_yaml = """
remoteok:
  enabled: true

adzuna:
  enabled: true
  base_url: https://api.adzuna.com/v1/api/jobs
  default_country: gb
  default_page_size: 50
  max_pages_per_run: 5
"""

    config_path = tmp_path / "sources.yaml"
    config_path.write_text(invalid_yaml, encoding="utf-8")

    monkeypatch.setattr(
        "config.source_config.SOURCE_CONFIG_PATH",
        config_path,
    )

    with pytest.raises(ValueError, match="base_url"):
        load_source_config()


def test_remoteok_ingestor_rejects_disabled_source(
    monkeypatch,
    tmp_path,
) -> None:
    disabled_config = SourceConfig(
        remoteok=RemoteOkSourceConfig(
            enabled=False,
            base_url="https://remoteok.com/api",
        ),
        adzuna=AdzunaSourceConfig(
            enabled=True,
            base_url="https://api.adzuna.com/v1/api/jobs",
            default_country="gb",
            default_page_size=50,
            max_pages_per_run=5,
        ),
    )

    monkeypatch.setattr(
        "src.ingestion.sources.remoteok.load_source_config",
        lambda: disabled_config,
    )

    with pytest.raises(ValueError, match="disabled"):
        RemoteOkIngestor(raw_data_directory=str(tmp_path))
