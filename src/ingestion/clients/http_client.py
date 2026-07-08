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


class HttpRequestError(Exception):
    """Raised when an HTTP request fails after all retry attempts."""


class HttpClient:
    """Reusable HTTP client for external API requests."""

    def __init__(self) -> None:
        settings = get_settings()

        self.timeout_seconds = settings.request_timeout_seconds
        self.max_retries = settings.max_retries

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
        """
        Send a GET request with timeout, retry, and clear error handling.
        """
        try:
            response = self.session.get(
                url,
                params=params,
                headers=headers,
                timeout=self.timeout_seconds,
            )
        except (requests.Timeout, requests.ConnectionError) as error:
            raise HttpRequestError(
                f"Network request failed for {url}: {error}"
            ) from error

        if response.status_code >= 400:
            raise HttpRequestError(
                f"HTTP {response.status_code} returned from {response.url}"
            )

        return response
