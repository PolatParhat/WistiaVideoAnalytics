"""WistiaClient: auth header, retries, pagination. HTTP is mocked with `responses`: no real calls."""

import pytest
import requests
import responses

from wistia.api import WistiaApiError, WistiaClient

BASE = "https://api.wistia.com/v1"
EVENTS = f"{BASE}/stats/events.json"


@pytest.fixture
def client():
    waits = []
    c = WistiaClient(token="test-token", per_page=2, sleep=waits.append)
    c.waits = waits  # lets a test see how long the client would have waited
    return c


@responses.activate
def test_sends_bearer_token(client):
    responses.get(f"{BASE}/medias/abc.json", json={"hashed_id": "abc"})
    assert client.media("abc") == {"hashed_id": "abc"}
    assert responses.calls[0].request.headers["Authorization"] == "Bearer test-token"


@responses.activate
def test_retries_5xx_then_succeeds(client):
    responses.get(f"{BASE}/medias/abc.json", status=503)
    responses.get(f"{BASE}/medias/abc.json", status=500)
    responses.get(f"{BASE}/medias/abc.json", json={"ok": True})
    assert client.media("abc") == {"ok": True}
    assert len(responses.calls) == 3
    assert len(client.waits) == 2
    assert 1 <= client.waits[0] < 2 and 2 <= client.waits[1] < 3  # exponential backoff


@responses.activate
def test_honours_retry_after_on_429(client):
    responses.get(f"{BASE}/medias/abc.json", status=429, headers={"Retry-After": "7"})
    responses.get(f"{BASE}/medias/abc.json", json={})
    client.media("abc")
    assert client.waits == [7.0]


@responses.activate
def test_retries_network_errors(client):
    responses.get(f"{BASE}/medias/abc.json", body=requests.ConnectionError("down"))
    responses.get(f"{BASE}/medias/abc.json", json={"ok": True})
    assert client.media("abc") == {"ok": True}


@responses.activate
def test_gives_up_after_max_attempts(client):
    for _ in range(5):
        responses.get(f"{BASE}/medias/abc.json", status=502)
    with pytest.raises(WistiaApiError, match="after 5 attempts"):
        client.media("abc")
    assert len(responses.calls) == 5


@responses.activate
@pytest.mark.parametrize("status", [401, 404])
def test_does_not_retry_client_errors(client, status):
    responses.get(f"{BASE}/medias/abc.json", status=status)
    with pytest.raises(WistiaApiError, match=str(status)):
        client.media("abc")
    assert len(responses.calls) == 1


def mock_events_page(page, rows):
    """Register a fake events response for one page number."""
    params = {"media_id": "abc", "start_date": "2026-03-17", "end_date": "2026-03-18"}
    query = {**params, "per_page": "2", "page": str(page)}
    responses.get(EVENTS, match=[responses.matchers.query_param_matcher(query)], json=rows)


@responses.activate
def test_pagination_reads_until_empty_page(client):
    mock_events_page(1, [{"n": 1}, {"n": 2}])
    mock_events_page(2, [{"n": 3}])
    mock_events_page(3, [])

    pages = list(client.event_pages("abc", "2026-03-17", "2026-03-18"))

    assert [p for p, _, _ in pages] == [1, 2]
    assert [row["n"] for _, rows, _ in pages for row in rows] == [1, 2, 3]
    assert len(responses.calls) == 3  # the empty third page ends the loop


@responses.activate
def test_pagination_with_no_data(client):
    responses.get(EVENTS, json=[])
    assert list(client.event_pages("abc", "2026-03-17", "2026-03-17")) == []
