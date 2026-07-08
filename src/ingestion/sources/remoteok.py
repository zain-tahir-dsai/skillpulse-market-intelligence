from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config.source_config import load_source_config
from src.ingestion.clients import HttpClient
from src.ingestion.logging_config import get_logger
from src.ingestion.utils import write_json_with_fingerprint


class RemoteOkIngestor:
    """Fetch and store the RemoteOK job-feed snapshot."""

    source_name = "remoteok"

    def __init__(
        self,
        base_url: str | None = None,
        raw_data_directory: str = "data/raw/remoteok",
        client: HttpClient | None = None,
    ) -> None:
        source_config = load_source_config().remoteok

        if not source_config.enabled:
            raise ValueError(
                "RemoteOK ingestion is disabled in config/sources.yaml."
            )

        self.base_url = base_url or source_config.base_url
        self.raw_data_directory = Path(raw_data_directory)
        self.client = client or HttpClient()
        self.logger = get_logger(__name__)

        self.last_payload_fingerprint: str | None = None
        self.last_created_new_raw_file: bool | None = None

    def fetch(self) -> list[dict[str, Any]]:
        """Fetch the full RemoteOK JSON feed."""
        response = self.client.get(self.base_url)
        payload = response.json()

        if not isinstance(payload, list):
            raise ValueError("RemoteOK response must be a JSON list.")

        return payload

    def save_raw_payload(
        self,
        payload: list[dict[str, Any]],
        ingested_at: datetime | None = None,
    ) -> Path:
        """Save the API response unchanged in the raw landing zone."""
        timestamp = ingested_at or datetime.now(timezone.utc)
        timestamp_text = timestamp.strftime("%Y%m%dT%H%M%SZ")

        output_path = (
            self.raw_data_directory
            / f"{self.source_name}_{timestamp_text}.json"
        )

        (
            saved_path,
            payload_fingerprint,
            created_new_raw_file,
        ) = write_json_with_fingerprint(output_path, payload)

        self.last_payload_fingerprint = payload_fingerprint
        self.last_created_new_raw_file = created_new_raw_file

        return saved_path

    def run(self) -> tuple[Path, int]:
        """Fetch, save, and return the output path and job record count."""
        payload = self.fetch()

        self.logger.info(
            "source_payload_fetched",
            extra={
                "extra_fields": {
                    "event": "source_payload_fetched",
                    "source": self.source_name,
                    "payload_record_count": len(payload),
                }
            },
        )

        output_path = self.save_raw_payload(payload)

        job_count = sum(
            1
            for record in payload
            if isinstance(record, dict) and "position" in record
        )

        self.logger.info(
            "source_ingestion_completed",
            extra={
                "extra_fields": {
                    "event": "source_ingestion_completed",
                    "source": self.source_name,
                    "job_record_count": job_count,
                    "raw_file": output_path.as_posix(),
                    "payload_fingerprint": self.last_payload_fingerprint,
                    "created_new_raw_file": self.last_created_new_raw_file,
                }
            },
        )

        return output_path, job_count
