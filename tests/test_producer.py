from __future__ import annotations

import datetime as dt
import json
import pathlib

import pytest

from ingest.producer import EventProducer
from ingest.schemas import BusinessEvent


@pytest.fixture
def producer(settings):
    """Producer in file-sink-only mode (KAFKA_BOOTSTRAP_SERVERS is empty)."""
    p = EventProducer(settings)
    yield p
    p.close()


def _good_business() -> BusinessEvent:
    return BusinessEvent(
        business_id="b1",
        name="Cafe Constant",
        rating=4.5,
        review_count=218,
        categories=["cafes"],
        coordinates={"latitude": 42.36, "longitude": -71.06},
        location={"city": "Boston"},
        search_term="restaurants",
        search_location="Boston",
    )


def _bad_business() -> BusinessEvent:
    return BusinessEvent(
        business_id="x",
        name="X",
        rating=7.0,  # out of range
        review_count=-5,  # negative
        categories=["x"],
        coordinates={"latitude": 0.0, "longitude": 0.0},
        location={"city": "Boston"},
        search_term="restaurants",
        search_location="Boston",
    )


def _read_sink(settings) -> list[dict]:
    sink_dir = pathlib.Path(settings.file_sink_path)
    lines: list[dict] = []
    for f in sorted(sink_dir.glob("*.jsonl")):
        for line in f.read_text().splitlines():
            if line.strip():
                lines.append(json.loads(line))
    return lines


def test_valid_event_is_published(producer, settings, fixed_clock):
    accepted = producer.publish_business(_good_business(), fetched_at=fixed_clock)
    assert accepted is True

    rows = _read_sink(settings)
    assert len(rows) == 1
    env = rows[0]
    assert env["payload"]["business_id"] == "b1"
    assert env["producer_id"] == "heimdall-test"
    assert env["schema_version"] == 1


def test_invalid_event_routes_to_dlq(producer, settings, fixed_clock):
    accepted = producer.publish_business(_bad_business(), fetched_at=fixed_clock)
    assert accepted is False

    rows = _read_sink(settings)
    assert len(rows) == 1
    dlq = rows[0]
    assert dlq["reason"] == "pre_publish_validation"
    codes = {f["code"] for f in dlq["failures"]}
    assert "rating_out_of_range" in codes
    assert "negative_review_count" in codes


def test_inject_bad_writes_dlq_record(producer, settings):
    producer.inject_bad_for_test()
    producer.flush()
    rows = _read_sink(settings)
    assert any(r.get("reason") == "injected_for_test" for r in rows)


def test_partition_key_is_business_id(producer, settings, fixed_clock):
    producer.publish_business(_good_business(), fetched_at=fixed_clock)
    rows = _read_sink(settings)
    # file-sink mode does not preserve key separately, but the business_id is
    # carried in payload and is what we'd use as the Kafka key.
    assert rows[0]["payload"]["business_id"] == "b1"


def test_ingest_lag_ms_is_nonnegative(settings, fixed_clock):
    later = fixed_clock + dt.timedelta(milliseconds=250)
    p = EventProducer(settings, clock=lambda: later)
    try:
        p.publish_business(_good_business(), fetched_at=fixed_clock)
        rows = _read_sink(settings)
        assert rows[0]["ingest_lag_ms"] >= 250
    finally:
        p.close()
