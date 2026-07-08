from datetime import datetime, timezone

import pytest
import responses

from src.ingestion.sources.adzuna import AdzunaIngestor


@pytest.fixture
def adzuna_env(monkeypatch) -> None:
    monkeypatch.setenv("ADZUNA_APP_ID", "test-app-id")
    monkeypatch.setenv("ADZUNA_APP_KEY", "test-app-key")


@responses.activate
def test_fetch_page_returns_adzuna_payload(adzuna_env) -> None:
    payload = {
        "count": 1,
        "results": [{"id": "job-1", "title": "Data Engineer"}],
    }

    responses.add(
        responses.GET,
        "https://api.adzuna.com/v1/api/jobs/gb/search/1",
        json=payload,
        status=200,
    )

    ingestor = AdzunaIngestor()
    result = ingestor.fetch_page(query="data engineer", page=1)

    assert result == payload


def test_save_raw_payload_creates_json_file(tmp_path, adzuna_env) -> None:
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
    assert (
        output_path.name
        == "adzuna_gb_data_engineer_page_1_20260708T083000Z.json"
    )


@responses.activate
def test_run_stops_after_short_page(tmp_path, adzuna_env) -> None:
    first_page = {
        "count": 2,
        "results": [
            {"id": "job-1", "title": "Data Engineer"},
            {"id": "job-2", "title": "Data Analyst"},
        ],
    }

    responses.add(
        responses.GET,
        "https://api.adzuna.com/v1/api/jobs/gb/search/1",
        json=first_page,
        status=200,
    )

    ingestor = AdzunaIngestor(
        raw_data_directory=str(tmp_path),
        default_page_size=50,
        max_pages_per_run=5,
    )

    saved_paths, total_records = ingestor.run(query="data")

    assert len(saved_paths) == 1
    assert saved_paths[0].exists()
    assert total_records == 2
