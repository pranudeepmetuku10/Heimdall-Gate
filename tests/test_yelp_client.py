from __future__ import annotations

import httpx
import pytest
import respx

from ingest.yelp_client import YelpAPIError, YelpClient


@respx.mock
def test_search_businesses_returns_payload(yelp_search_body):
    route = respx.get("https://api.yelp.com/v3/businesses/search").mock(
        return_value=httpx.Response(200, json=yelp_search_body)
    )

    with YelpClient(api_key="k") as client:
        body = client.search_businesses(location="Boston", term="restaurants")

    assert route.called
    assert body["businesses"][0]["id"] == "biz-1"
    assert client.calls_made == 1


@respx.mock
def test_iter_search_pages_until_short_page(yelp_search_body):
    short = {"total": 2, "businesses": [yelp_search_body["businesses"][0]]}
    respx.get("https://api.yelp.com/v3/businesses/search").mock(
        side_effect=[
            httpx.Response(200, json=yelp_search_body),
            httpx.Response(200, json=short),
        ]
    )

    with YelpClient(api_key="k") as client:
        out = list(
            client.iter_search(
                location="Boston", term="restaurants", page_size=2, max_pages=3
            )
        )

    # 2 from first page + 1 from short page; iteration stops on short page.
    assert len(out) == 3
    assert client.calls_made == 2


@respx.mock
def test_4xx_other_than_429_raises_immediately():
    respx.get("https://api.yelp.com/v3/businesses/search").mock(
        return_value=httpx.Response(400, json={"error": {"code": "VALIDATION_ERROR"}})
    )

    with YelpClient(api_key="k") as client:
        with pytest.raises(YelpAPIError) as exc:
            client.search_businesses(location="Boston", term="restaurants")
    assert exc.value.status == 400


@respx.mock
def test_5xx_is_retried_then_succeeds(yelp_search_body):
    respx.get("https://api.yelp.com/v3/businesses/search").mock(
        side_effect=[
            httpx.Response(503, text="overloaded"),
            httpx.Response(200, json=yelp_search_body),
        ]
    )

    with YelpClient(api_key="k") as client:
        body = client.search_businesses(location="Boston", term="restaurants")

    assert body["total"] == 2
    assert client.calls_made == 2  # both attempts counted


@respx.mock
def test_budget_exhaustion_stops_iteration(yelp_search_body):
    respx.get("https://api.yelp.com/v3/businesses/search").mock(
        return_value=httpx.Response(200, json=yelp_search_body),
    )

    with YelpClient(api_key="k", api_call_budget=1) as client:
        out = list(
            client.iter_search(
                location="Boston", term="restaurants", page_size=2, max_pages=5
            )
        )
    # First page yields 2 items; second page would exceed budget and stop the iter.
    assert len(out) == 2
    assert client.calls_made == 1


def test_empty_api_key_rejected():
    with pytest.raises(ValueError):
        YelpClient(api_key="")
