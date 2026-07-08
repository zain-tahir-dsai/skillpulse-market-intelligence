from unittest.mock import Mock

import pytest
import requests

from src.ingestion.clients.http_client import HttpClient, HttpRequestError


def test_get_returns_successful_response(monkeypatch) -> None:
    client = HttpClient()

    response = Mock()
    response.status_code = 200
    response.url = "https://example.com/api"

    monkeypatch.setattr(client.session, "get", Mock(return_value=response))

    result = client.get("https://example.com/api")

    assert result == response


def test_get_raises_error_for_http_failure(monkeypatch) -> None:
    client = HttpClient()

    response = Mock()
    response.status_code = 500
    response.url = "https://example.com/api"

    monkeypatch.setattr(client.session, "get", Mock(return_value=response))

    with pytest.raises(HttpRequestError, match="HTTP 500"):
        client.get("https://example.com/api")


def test_get_raises_error_for_connection_failure(monkeypatch) -> None:
    client = HttpClient()

    monkeypatch.setattr(
        client.session,
        "get",
        Mock(side_effect=requests.ConnectionError("Connection refused")),
    )

    with pytest.raises(HttpRequestError, match="Network request failed"):
        client.get("https://example.com/api")