from pathlib import Path

from src.ingestion import run_ingestion


def test_write_run_log_creates_json_file(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(run_ingestion, "RUN_LOG_DIRECTORY", tmp_path)

    metadata = {
        "source": "remoteok",
        "status": "success",
        "record_count": 100,
        "raw_files": ["data/raw/remoteok/example.json"],
    }

    output_path = run_ingestion.write_run_log(metadata)

    assert output_path.exists()
    assert output_path.parent == tmp_path
    assert "remoteok_" in output_path.name


def test_run_remoteok_returns_failure_metadata_when_ingestion_fails(
    monkeypatch,
) -> None:
    class FailingRemoteOkIngestor:
        def run(self):
            raise RuntimeError("RemoteOK is unavailable")

    monkeypatch.setattr(
        run_ingestion,
        "RemoteOkIngestor",
        FailingRemoteOkIngestor,
    )

    result = run_ingestion.run_remoteok()

    assert result["source"] == "remoteok"
    assert result["status"] == "failed"
    assert result["record_count"] == 0
    assert result["error"] == "RemoteOK is unavailable"


def test_run_adzuna_returns_success_metadata(monkeypatch) -> None:
    class SuccessfulAdzunaIngestor:
        def run(self, **kwargs):
            return [Path("data/raw/adzuna/example.json")], 5

    monkeypatch.setattr(
        run_ingestion,
        "AdzunaIngestor",
        SuccessfulAdzunaIngestor,
    )

    result = run_ingestion.run_adzuna(
        query="data engineer",
        country="gb",
        page_size=5,
        max_pages=1,
    )

    assert result["source"] == "adzuna"
    assert result["status"] == "success"
    assert result["record_count"] == 5
    assert result["raw_files"] == ["data/raw/adzuna/example.json"]


def test_run_source_routes_to_remoteok(monkeypatch) -> None:
    monkeypatch.setattr(
        run_ingestion,
        "run_remoteok",
        lambda: {
            "source": "remoteok",
            "status": "success",
            "record_count": 1,
            "raw_files": ["data/raw/remoteok/example.json"],
        },
    )

    result = run_ingestion.run_source("remoteok")

    assert result["source"] == "remoteok"


def test_run_source_routes_to_adzuna(monkeypatch) -> None:
    monkeypatch.setattr(
        run_ingestion,
        "run_adzuna",
        lambda **kwargs: {
            "source": "adzuna",
            "status": "success",
            "record_count": 2,
            "raw_files": ["data/raw/adzuna/example.json"],
        },
    )

    result = run_ingestion.run_source(
        "adzuna",
        query="data engineer",
        country="gb",
        page_size=10,
        max_pages=1,
    )

    assert result["source"] == "adzuna"
