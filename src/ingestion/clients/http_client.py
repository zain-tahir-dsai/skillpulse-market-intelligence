from __future__ import annotations

from typing import Any

import requests
from requests import Response
from requests.adapters import HTTPAdapter
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)
from urllib3.util.retry import Retry

from config.settings import get_settings
from src.ingestion.logging_config import get_logger


class HttpRequestError(Exception):
    """Raised when an HTTP request fails after all retry attempts."""


class HttpClient:
    """Reusable HTTP client for external API requests."""

    def __init__(self) -> None:
        settings = get_settings()

        self.timeout_seconds = settings.request_timeout_seconds
        self.max_retries = settings.max_retries
        self.logger = get_logger(__name__)

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "SkillPulse/0.1 (local development)",
                "Accept": "application/json",
            }
        )

        retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _safe_params_for_logging(
        self,
        params: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """Return request parameters with secrets masked for logs."""
        safe_params = dict(params or {})

        for secret_key in ("app_id", "app_key", "api_key", "token"):
            if secret_key in safe_params:
                safe_params[secret_key] = "***"

        return safe_params

    @retry(
        retry=retry_if_exception_type(
            (requests.Timeout, requests.ConnectionError)
        ),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        stop=stop_after_attempt(3),
        reraise=True,
    )
    def get(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> Response:
        """Send a GET request with timeout, retry, and structured logs."""
        self.logger.info(
            "http_request_started",
            extra={
                "extra_fields": {
                    "event": "http_request_started",
                    "method": "GET",
                    "url": url,
                    "params": self._safe_params_for_logging(params),
                }
            },
        )

        try:
            response = self.session.get(
                url,
                params=params,
                headers=headers,
                timeout=self.timeout_seconds,
            )
        except (requests.Timeout, requests.ConnectionError) as error:
            self.logger.error(
                "http_request_failed",
                extra={
                    "extra_fields": {
                        "event": "http_request_failed",
                        "method": "GET",
                        "url": url,
                        "error_type": type(error).__name__,
                        "error_message": str(error),
                    }
                },
                exc_info=True,
            )
            raise HttpRequestError(
                f"Network request failed for {url}: {error}"
            ) from error

        if response.status_code >= 400:
            error = HttpRequestError(
                f"HTTP {response.status_code} returned from {response.url}"
            )

            self.logger.error(
                "http_request_failed",
                extra={
                    "extra_fields": {
                        "event": "http_request_failed",
                        "method": "GET",
                        "url": url,
                        "status_code": response.status_code,
                        "error_type": type(error).__name__,
                        "error_message": str(error),
                    }
                },
            )
            raise error

        self.logger.info(
            "http_request_succeeded",
            extra={
                "extra_fields": {
                    "event": "http_request_succeeded",
                    "method": "GET",
                    "url": url,
                    "status_code": response.status_code,
                }
            },
        )

        return response
