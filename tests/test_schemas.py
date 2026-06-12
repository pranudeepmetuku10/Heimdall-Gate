from __future__ import annotations

import datetime as dt

from ingest.schemas import BusinessEvent, Envelope


def test_business_event_from_yelp_extracts_category_aliases(yelp_search_body):
    body = yelp_search_body["businesses"][0]
    event = BusinessEvent.from_yelp(
        body, search_term="restaurants", search_location="Boston"
    )

    assert event.business_id == "biz-1"
    assert event.name == "Cafe Constant"
    assert event.categories == ["cafes"]
    assert event.coordinates.latitude == 42.3601
    assert event.coordinates.longitude == -71.0589
    assert event.location.city == "Boston"
    assert event.search_term == "restaurants"
    assert event.search_location == "Boston"


def test_business_event_handles_missing_optional_fields():
    body = {
        "id": "x",
        "name": "X",
        "rating": 4.0,
        "review_count": 1,
        "categories": [{"alias": "bars", "title": "Bars"}],
        "coordinates": {"latitude": 0.0, "longitude": 0.0},
        "location": {"city": "Boston"},
    }
    event = BusinessEvent.from_yelp(body, search_term="t", search_location="Boston")
    assert event.url is None
    assert event.transactions == []


def test_envelope_round_trip_serializes_to_json(fixed_clock):
    event = BusinessEvent(
        business_id="b1",
        name="N",
        rating=4.0,
        review_count=10,
        categories=["cafes"],
        coordinates={"latitude": 1.0, "longitude": 2.0},
        location={"city": "Boston"},
        search_term="restaurants",
        search_location="Boston",
    )
    env = Envelope(
        producer_id="p",
        observed_at=fixed_clock,
        payload=event,
    )
    blob = env.model_dump_json()
    again = Envelope.model_validate_json(blob)
    assert again.payload.business_id == "b1"
    assert again.kafka_key() == "b1"
    assert again.observed_at.tzinfo is not None


def test_envelope_observed_at_coerces_naive_to_utc():
    naive = dt.datetime(2026, 1, 1, 12, 0, 0)
    event = BusinessEvent(
        business_id="b",
        name="N",
        rating=4.0,
        review_count=1,
        categories=["c"],
        coordinates={"latitude": 0.0, "longitude": 0.0},
        location={"city": "Boston"},
        search_term="t",
        search_location="Boston",
    )
    env = Envelope(producer_id="p", observed_at=naive, payload=event)
    assert env.observed_at.tzinfo is dt.UTC
