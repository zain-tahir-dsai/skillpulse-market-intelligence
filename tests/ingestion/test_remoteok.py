from datetime import datetime, timezone

import responses

from src.ingestion.sources.remoteok import RemoteOkIngestor


@responses.activate
def test_fetch_returns_remoteok_payload() -> None:
    payload = [
        {"legal": "RemoteOK API metadata"},
        {"id": "job-1", "position": "Data Engineer"},
    ]

    responses.add(
        responses.GET,
        "https://remoteok.com/api",
        json=payload,
        status=200,
    )

    ingestor = RemoteOkIngestor()
    result = ingestor.fetch()

    assert result == payload


def test_save_raw_payload_creates_json_file(tmp_path) -> None:
    ingestor = RemoteOkIngestor(raw_data_directory=str(tmp_path))
    payload = [{"id": "job-1", "position": "Data Engineer"}]
    fixed_time = datetime(2026, 7, 8, 8, 0, 0, tzinfo=timezone.utc)

    output_path = ingestor.save_raw_payload(
        payload=payload,
        ingested_at=fixed_time,
    )

    assert output_path.exists()
    assert output_path.name == "remoteok_20260708T080000Z.json"
    assert output_path.read_text(encoding="utf-8")


@responses.activate
def test_run_returns_saved_path_and_job_count(tmp_path) -> None:
    payload = [
        {"legal": "RemoteOK API metadata"},
        {"id": "job-1", "position": "Data Engineer"},
        {"id": "job-2", "position": "Data Analyst"},
    ]

    responses.add(
        responses.GET,
        "https://remoteok.com/api",
        json=payload,
        status=200,
    )

    ingestor = RemoteOkIngestor(raw_data_directory=str(tmp_path))
    output_path, job_count = ingestor.run()

    assert output_path.exists()
    assert job_count == 2
