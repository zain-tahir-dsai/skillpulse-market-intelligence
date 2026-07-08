import pytest

from src.ingestion.utils import RawFileAlreadyExistsError, write_json_once


def test_write_json_once_creates_file(tmp_path) -> None:
    output_path = tmp_path / "sample.json"
    payload = {"source": "test", "records": 1}

    result = write_json_once(output_path, payload)

    assert result == output_path
    assert output_path.exists()


def test_write_json_once_prevents_duplicate_file(tmp_path) -> None:
    output_path = tmp_path / "sample.json"
    payload = {"source": "test", "records": 1}

    write_json_once(output_path, payload)

    with pytest.raises(RawFileAlreadyExistsError, match="Raw file already exists"):
        write_json_once(output_path, payload)
    