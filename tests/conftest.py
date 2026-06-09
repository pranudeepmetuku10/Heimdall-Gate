"""Shared test fixtures.

Tests are designed to run without network or Kafka. They use respx to stub
httpx requests and a fake confluent_kafka producer for the publisher.
"""

from __future__ import annotations

import datetime as dt
from typing import Any

import pytest

from config.settings import Settings


@pytest.fixture
def settings(tmp_path) -> Settings:
    return Settings(
        YELP_API_KEY="test-key",
        YELP_BASE_URL="https://api.yelp.com/v3",
        HEIMDALL_CITIES="Boston",
        HEIMDALL_TERMS="restaurants",
        HEIMDALL_PAGE_SIZE=2,
        HEIMDALL_MAX_PAGES_PER_CITY=2,
        HEIMDALL_POLL_INTERVAL_SEC=60,
        HEIMDALL_API_CALL_BUDGET=10,
        KAFKA_BOOTSTRAP_SERVERS="",  # disable real producer in tests
        KAFKA_TOPIC_RAW="heimdall.raw.business",
        KAFKA_TOPIC_DLQ="heimdall.dlq.ingest",
        HEIMDALL_FILE_SINK_ENABLED=True,
        HEIMDALL_FILE_SINK_PATH=str(tmp_path / "sink"),
        HEIMDALL_PRODUCER_ID="heimdall-test",
        HEIMDALL_LOG_LEVEL="WARNING",
        HEIMDALL_LOG_FORMAT="console",
    )


@pytest.fixture
def yelp_search_body() -> dict[str, Any]:
    """Realistic shape (trimmed) of a /businesses/search response."""
    return {
        "total": 2,
        "businesses": [
            {
                "id": "biz-1",
                "name": "Cafe Constant",
                "url": "https://yelp.example/biz/cafe-constant",
                "image_url": "https://yelp.example/img/1.jpg",
                "is_closed": False,
                "rating": 4.5,
                "review_count": 218,
                "price": "$$",
                "phone": "+16175551111",
                "categories": [{"alias": "cafes", "title": "Cafes"}],
                "coordinates": {"latitude": 42.3601, "longitude": -71.0589},
                "location": {
                    "address1": "1 Beacon St",
                    "city": "Boston",
                    "state": "MA",
                    "country": "US",
                    "zip_code": "02108",
                },
                "transactions": ["delivery"],
            },
            {
                "id": "biz-2",
                "name": "Harborline Grill",
                "url": "https://yelp.example/biz/harborline",
                "image_url": "https://yelp.example/img/2.jpg",
                "is_closed": False,
                "rating": 3.5,
                "review_count": 47,
                "price": "$$$",
                "phone": "+16175552222",
                "categories": [{"alias": "newamerican", "title": "American (New)"}],
                "coordinates": {"latitude": 42.3554, "longitude": -71.0640},
                "location": {
                    "address1": "55 Atlantic Ave",
                    "city": "Boston",
                    "state": "MA",
                    "country": "US",
                    "zip_code": "02110",
                },
                "transactions": [],
            },
        ],
    }


@pytest.fixture
def fixed_clock() -> dt.datetime:
    return dt.datetime(2026, 6, 9, 12, 0, 0, tzinfo=dt.timezone.utc)
