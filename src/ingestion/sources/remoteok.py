from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.ingestion.clients import HttpClient


class RemoteOkIngestor:
    """Fetch and store the RemoteOK job-feed snapshot."""

    source_name = "remoteok"

    def __init__(
        self,
        base_url: str = "https://remoteok.com/api",
        raw_data_directory: str = "data/raw/remoteok",
        client: HttpClient | None = None,
    ) -> None:
        self.base_url = base_url
        self.raw_data_directory = Path(raw_data_directory)
        self.client = client or HttpClient()

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

        self.raw_data_directory.mkdir(parents=True, exist_ok=True)

        output_path = (
            self.raw_data_directory
            / f"{self.source_name}_{timestamp_text}.json"
        )

        with output_path.open("w", encoding="utf-8") as raw_file:
            json.dump(payload, raw_file, ensure_ascii=False, indent=2)

        return output_path

    def run(self) -> tuple[Path, int]:
        """Fetch, save, and return the output path and record count."""
        payload = self.fetch()
        output_path = self.save_raw_payload(payload)

        # RemoteOK can include one metadata record before job records.
        job_count = sum(
            1
            for record in payload
            if isinstance(record, dict) and "position" in record
        )

        return output_path, job_count
