from __future__ import annotations

import httpx
import respx

from ingest.runner import Runner
from ingest.producer import EventProducer
from ingest.yelp_client import YelpClient


@respx.mock
def test_run_once_publishes_each_business(settings, yelp_search_body):
    short_page = {"total": 2, "businesses": []}
    respx.get("https://api.yelp.com/v3/businesses/search").mock(
        side_effect=[
            httpx.Response(200, json=yelp_search_body),
            httpx.Response(200, json=short_page),
        ]
    )

    client = YelpClient(
        api_key=settings.yelp_api_key,
        base_url=settings.yelp_base_url,
        timeout=settings.yelp_request_timeout_sec,
        api_call_budget=settings.api_call_budget,
    )
    producer = EventProducer(settings)
    try:
        runner = Runner(settings, client=client, producer=producer)
        stats = runner.run_once(cities=["Boston"], terms=["restaurants"])
    finally:
        producer.close()
        client.close()

    assert stats.businesses_seen == 2
    assert stats.accepted == 2
    assert stats.rejected == 0
    assert stats.errors == []
