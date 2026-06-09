from __future__ import annotations

from validation.validators import validate_business_event


def _good_event() -> dict:
    return {
        "business_id": "abc",
        "name": "Test",
        "rating": 4.0,
        "review_count": 10,
        "categories": ["cafes"],
        "coordinates": {"latitude": 42.0, "longitude": -71.0},
        "location": {"city": "Boston"},
    }


def test_valid_event_has_no_failures():
    assert validate_business_event(_good_event()) == []


def test_missing_business_id_is_flagged():
    bad = _good_event()
    bad["business_id"] = ""
    codes = [f.code for f in validate_business_event(bad)]
    assert "missing_business_id" in codes


def test_rating_out_of_range_is_flagged():
    bad = _good_event()
    bad["rating"] = 7.0
    codes = [f.code for f in validate_business_event(bad)]
    assert "rating_out_of_range" in codes


def test_rating_must_be_half_step():
    bad = _good_event()
    bad["rating"] = 4.2
    codes = [f.code for f in validate_business_event(bad)]
    assert "rating_out_of_range" in codes


def test_negative_review_count_flagged():
    bad = _good_event()
    bad["review_count"] = -1
    codes = [f.code for f in validate_business_event(bad)]
    assert "negative_review_count" in codes


def test_lat_lng_out_of_range_flagged():
    bad = _good_event()
    bad["coordinates"] = {"latitude": 100.0, "longitude": -200.0}
    codes = [f.code for f in validate_business_event(bad)]
    assert "latitude_out_of_range" in codes
    assert "longitude_out_of_range" in codes


def test_missing_city_flagged():
    bad = _good_event()
    bad["location"] = {"city": ""}
    codes = [f.code for f in validate_business_event(bad)]
    assert "missing_city" in codes


def test_empty_categories_flagged():
    bad = _good_event()
    bad["categories"] = []
    codes = [f.code for f in validate_business_event(bad)]
    assert "missing_categories" in codes


def test_multiple_failures_accumulate():
    bad = {
        "business_id": "",
        "name": "",
        "rating": 99,
        "review_count": -1,
        "categories": [],
        "coordinates": {"latitude": 999, "longitude": 999},
        "location": {"city": ""},
    }
    codes = {f.code for f in validate_business_event(bad)}
    assert {
        "missing_business_id",
        "missing_name",
        "rating_out_of_range",
        "negative_review_count",
        "missing_categories",
        "latitude_out_of_range",
        "longitude_out_of_range",
        "missing_city",
    }.issubset(codes)
