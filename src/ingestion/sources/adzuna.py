from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config.settings import get_settings
from src.ingestion.clients import HttpClient
from src.ingestion.logging_config import get_logger
from src.ingestion.utils import write_json_once


class AdzunaIngestor:
    """Fetch and store paginated Adzuna job-search responses."""

    source_name = "adzuna"

    def __init__(
        self,
        base_url: str = "https://api.adzuna.com/v1/api/jobs",
        raw_data_directory: str = "data/raw/adzuna",
        default_country: str = "gb",
        default_page_size: int = 50,
        max_pages_per_run: int = 5,
        client: HttpClient | None = None,
    ) -> None:
        settings = get_settings()

        self.base_url = base_url.rstrip("/")
        self.raw_data_directory = Path(raw_data_directory)
        self.default_country = default_country
        self.default_page_size = default_page_size
        self.max_pages_per_run = max_pages_per_run
        self.client = client or HttpClient()
        self.logger = get_logger(__name__)

        self.app_id = settings.adzuna_app_id
        self.app_key = settings.adzuna_app_key

    def _build_url(self, country: str, page: int) -> str:
        return f"{self.base_url}/{country}/search/{page}"

    @staticmethod
    def _safe_query_text(query: str) -> str:
        normalized = re.sub(r"[^a-z0-9]+", "_", query.lower()).strip("_")
        return normalized or "all_jobs"

    def fetch_page(
        self,
        query: str,
        page: int,
        country: str | None = None,
        page_size: int | None = None,
        category: str | None = None,
    ) -> dict[str, Any]:
        """Fetch one Adzuna result page."""
        if not self.app_id or not self.app_key:
            raise ValueError(
                "Adzuna credentials are missing. Set ADZUNA_APP_ID and "
                "ADZUNA_APP_KEY in .env."
            )

        selected_country = country or self.default_country
        selected_page_size = page_size or self.default_page_size

        params: dict[str, Any] = {
            "app_id": self.app_id,
            "app_key": self.app_key,
            "results_per_page": selected_page_size,
            "what": query,
        }

        if category:
            params["category"] = category

        response = self.client.get(
            url=self._build_url(selected_country, page),
            params=params,
        )
        payload = response.json()

        if not isinstance(payload, dict):
            raise ValueError("Adzuna response must be a JSON object.")

        if "results" not in payload:
            raise ValueError("Adzuna response is missing the 'results' field.")

        if not isinstance(payload["results"], list):
            raise ValueError("Adzuna 'results' field must be a JSON list.")

        return payload

    def save_raw_payload(
        self,
        payload: dict[str, Any],
        query: str,
        country: str,
        page: int,
        ingested_at: datetime | None = None,
    ) -> Path:
        """Save one API response unchanged in the raw landing zone."""
        timestamp = ingested_at or datetime.now(timezone.utc)
        timestamp_text = timestamp.strftime("%Y%m%dT%H%M%SZ")
        safe_query = self._safe_query_text(query)

        output_path = self.raw_data_directory / (
            f"{self.source_name}_{country}_{safe_query}_"
            f"page_{page}_{timestamp_text}.json"
        )

        return write_json_once(output_path, payload)

    def run(
        self,
        query: str,
        country: str | None = None,
        page_size: int | None = None,
        max_pages: int | None = None,
        category: str | None = None,
    ) -> tuple[list[Path], int]:
        """
        Fetch Adzuna pages until the configured limit or an empty page.

        Returns saved file paths and the total number of fetched job records.
        """
        selected_country = country or self.default_country
        selected_max_pages = max_pages or self.max_pages_per_run

        saved_paths: list[Path] = []
        total_records = 0

        for page in range(1, selected_max_pages + 1):
            payload = self.fetch_page(
                query=query,
                page=page,
                country=selected_country,
                page_size=page_size,
                category=category,
            )

            results = payload["results"]

            self.logger.info(
                "source_page_fetched",
                extra={
                    "extra_fields": {
                        "event": "source_page_fetched",
                        "source": self.source_name,
                        "query": query,
                        "country": selected_country,
                        "page": page,
                        "page_record_count": len(results),
                    }
                },
            )

            if not results:
                break

            output_path = self.save_raw_payload(
                payload=payload,
                query=query,
                country=selected_country,
                page=page,
            )

            saved_paths.append(output_path)
            total_records += len(results)

            if len(results) < (page_size or self.default_page_size):
                break

        self.logger.info(
            "source_ingestion_completed",
            extra={
                "extra_fields": {
                    "event": "source_ingestion_completed",
                    "source": self.source_name,
                    "query": query,
                    "country": selected_country,
                    "pages_saved": len(saved_paths),
                    "job_record_count": total_records,
                }
            },
        )

        return saved_paths, total_records
