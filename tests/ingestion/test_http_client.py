from __future__ import annotations

import requests
import responses

from src.ingestion.clients.http_client import HttpClient, HttpRequestError


@responses.activate
def test_get_returns_successful_response() -> None:
    responses.add(
        responses.GET,
        "https://example.com/jobs",
        json={"status": "ok"},
        status=200,
    )

    client = HttpClient()

    response = client.get("https://example.com/jobs")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@responses.activate
def test_get_raises_error_for_http_failure() -> None:
    responses.add(
        responses.GET,
        "https://example.com/jobs",
        json={"error": "not found"},
        status=404,
    )

    client = HttpClient()

    try:
        client.get("https://example.com/jobs")
    except HttpRequestError as error:
        assert "HTTP 404" in str(error)
    else:
        raise AssertionError("HttpRequestError was not raised.")


def test_get_raises_error_for_connection_failure(monkeypatch) -> None:
    client = HttpClient()

    def raise_connection_error(*args, **kwargs):
        raise requests.ConnectionError("network unavailable")

    monkeypatch.setattr(client.session, "get", raise_connection_error)

    try:
        client.get("https://example.com/jobs")
    except HttpRequestError as error:
        assert "Network request failed" in str(error)
    else:
        raise AssertionError("HttpRequestError was not raised.")


def test_safe_params_for_logging_masks_secrets() -> None:
    client = HttpClient()

    safe_params = client._safe_params_for_logging(
        {
            "app_id": "real-app-id",
            "app_key": "real-app-key",
            "what": "data engineer",
        }
    )

    assert safe_params["app_id"] == "***"
    assert safe_params["app_key"] == "***"
    assert safe_params["what"] == "data engineer"
