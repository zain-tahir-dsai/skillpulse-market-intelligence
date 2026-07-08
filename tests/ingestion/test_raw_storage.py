from pathlib import Path
from src.ingestion.utils import (
    payload_fingerprint,
    write_json_once,
    write_json_with_fingerprint,
)


def test_write_json_once_creates_file(tmp_path: Path) -> None:
    output_path = tmp_path / "raw" / "example.json"
    payload = {"job_id": 123, "title": "Data Engineer"}

    saved_path = write_json_once(output_path, payload)

    assert saved_path == output_path
    assert output_path.exists()


def test_payload_fingerprint_is_stable_for_same_content() -> None:
    first_payload = {"title": "Data Engineer", "job_id": 123}
    second_payload = {"job_id": 123, "title": "Data Engineer"}

    first_fingerprint = payload_fingerprint(first_payload)
    second_fingerprint = payload_fingerprint(second_payload)

    assert first_fingerprint == second_fingerprint


def test_write_json_with_fingerprint_reuses_existing_payload(
        tmp_path: Path) -> None:
    raw_directory = tmp_path / "raw"
    first_path = raw_directory / "first.json"
    second_path = raw_directory / "second.json"
    payload = {"job_id": 123, "title": "Data Engineer"}

    first_result = write_json_with_fingerprint(first_path, payload)
    second_result = write_json_with_fingerprint(second_path, payload)

    saved_first_path, first_fingerprint, first_created = first_result
    saved_second_path, second_fingerprint, second_created = second_result

    assert saved_first_path == first_path
    assert first_created is True
    assert saved_second_path == first_path
    assert second_created is False
    assert first_fingerprint == second_fingerprint
    assert first_path.exists()
    assert not second_path.exists()
