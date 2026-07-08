import pytest
import requests
import responses

from src.ingestion.clients.http_client import HttpClient, HttpRequestError


@responses.activate
def test_get_returns_successful_response() -> None:
    client = HttpClient()

    responses.add(
        responses.GET,
        "https://example.com/jobs",
        json={"status": "ok"},
        status=200,
    )

    response = client.get("https://example.com/jobs")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@responses.activate
def test_get_raises_error_for_http_failure() -> None:
    client = HttpClient()

    responses.add(
        responses.GET,
        "https://example.com/jobs",
        json={"error": "not found"},
        status=404,
    )

    with pytest.raises(HttpRequestError, match="HTTP 404"):
        client.get("https://example.com/jobs")


def test_get_raises_error_for_connection_failure() -> None:
    client = HttpClient()

    with pytest.raises(HttpRequestError, match="Network request failed"):
        client.get("https://unreachable.example.com")
