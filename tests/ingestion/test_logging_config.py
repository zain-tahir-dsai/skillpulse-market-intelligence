import json
import logging

from src.ingestion.logging_config import JsonFormatter


def test_json_formatter_returns_valid_json() -> None:
    formatter = JsonFormatter()
    record = logging.LogRecord(
        name="skillpulse.test",
        level=logging.INFO,
        pathname="test_logging_config.py",
        lineno=10,
        msg="request completed",
        args=(),
        exc_info=None,
    )
    record.extra_fields = {
        "source": "remoteok",
        "record_count": 100,
    }

    formatted_log = formatter.format(record)
    parsed_log = json.loads(formatted_log)

    assert parsed_log["level"] == "INFO"
    assert parsed_log["logger"] == "skillpulse.test"
    assert parsed_log["message"] == "request completed"
    assert parsed_log["source"] == "remoteok"
    assert parsed_log["record_count"] == 100
    assert "timestamp" in parsed_log
