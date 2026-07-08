from pathlib import Path

import pytest

from config.source_config import load_source_config


def test_load_source_config_reads_valid_yaml(tmp_path: Path) -> None:
    config_file = tmp_path / "sources.yaml"
    config_file.write_text(
        """
adzuna:
  enabled: true
  base_url: "https://example.com/adzuna"
  default_country: "pk"
  default_page_size: 25
  max_pages_per_run: 3

remoteok:
  enabled: false
  base_url: "https://example.com/remoteok"
""".strip(),
        encoding="utf-8",
    )

    config = load_source_config(config_file)

    assert config.adzuna.enabled is True
    assert config.adzuna.default_country == "pk"
    assert config.adzuna.default_page_size == 25
    assert config.adzuna.max_pages_per_run == 3
    assert config.remoteok.enabled is False
    assert config.remoteok.base_url == "https://example.com/remoteok"


def test_load_source_config_raises_for_missing_required_field(
    tmp_path: Path,
) -> None:
    config_file = tmp_path / "sources.yaml"
    config_file.write_text(
        """
adzuna:
  enabled: true
  base_url: "https://example.com/adzuna"
  default_country: "pk"
  default_page_size: 25

remoteok:
  enabled: true
  base_url: "https://example.com/remoteok"
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="max_pages_per_run"):
        load_source_config(config_file)
