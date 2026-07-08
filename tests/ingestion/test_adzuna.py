from __future__ import annotations

from datetime import datetime, timezone
import pytest
from src.ingestion.sources.adzuna import AdzunaIngestor


@pytest.fixture
def adzuna_env(monkeypatch) -> None:
    monkeypatch.setenv("ADZUNA_APP_ID", "test-app-id")
    monkeypatch.setenv("ADZUNA_APP_KEY", "test-app-key")

    from config.settings import get_settings

    get_settings.cache_clear()


def test_fetch_page_returns_adzuna_payload(adzuna_env) -> None:
    payload = {
        "count": 1,
        "results": [{"id": "job-1", "title": "Data Engineer"}],
    }

    class FakeResponse:
        def json(self):
            return payload

    class FakeClient:
        def get(self, *args, **kwargs):
            return FakeResponse()

    ingestor = AdzunaIngestor(client=FakeClient())

    result = ingestor.fetch_page(
        query="data engineer",
        page=1,
        country="gb",
        page_size=5,
    )

    assert result == payload


def test_save_raw_payload_creates_json_file(
    tmp_path,
    adzuna_env,
) -> None:
    ingestor = AdzunaIngestor(raw_data_directory=str(tmp_path))
    payload = {
        "count": 1,
        "results": [{"id": "job-1", "title": "Data Engineer"}],
    }
    fixed_time = datetime(2026, 7, 8, 8, 30, 0, tzinfo=timezone.utc)

    output_path = ingestor.save_raw_payload(
        payload=payload,
        query="Data Engineer",
        country="gb",
        page=1,
        ingested_at=fixed_time,
    )

    assert output_path.exists()
    assert output_path.name.startswith("adzuna_gb_data_engineer_page_1_")


def test_run_stops_after_short_page(
    tmp_path,
    adzuna_env,
) -> None:
    first_page_payload = {
        "count": 2,
        "results": [
            {"id": "job-1", "title": "Data Engineer"},
            {"id": "job-2", "title": "Data Analyst"},
        ],
    }

    class FakeResponse:
        def json(self):
            return first_page_payload

    class FakeClient:
        def get(self, *args, **kwargs):
            return FakeResponse()

    ingestor = AdzunaIngestor(
        raw_data_directory=str(tmp_path),
        client=FakeClient(),
    )

    paths, count = ingestor.run(
        query="data engineer",
        country="gb",
        page_size=5,
        max_pages=3,
    )

    assert len(paths) == 1
    assert count == 2


def test_fetch_page_rejects_payload_without_results(
    adzuna_env,
) -> None:
    class FakeResponse:
        def json(self):
            return {"count": 0}

    class FakeClient:
        def get(self, *args, **kwargs):
            return FakeResponse()

    ingestor = AdzunaIngestor(client=FakeClient())

    with pytest.raises(ValueError, match="missing the 'results' field"):
        ingestor.fetch_page(query="data engineer", page=1)
