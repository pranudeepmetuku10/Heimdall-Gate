"""Shared validation rules for business events.

These rules are applied in two places:

  1. The Python producer applies them before publishing. Records that fail go
     to the producer-side DLQ topic.

  2. The Spark Silver job re-applies an equivalent set of rules (re-implemented
     in PySpark predicates because the validator is per-row Python). Failures
     there land in the DLQ Delta table.

Keeping the canonical rule set in one place makes drift between the two layers
visible during code review.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

# --- Sentinel set of "obviously bogus" rating values -------------------------
# Yelp ratings are 1.0..5.0 in 0.5 increments. Anything outside this set is a
# strong signal of corruption upstream.
_VALID_RATINGS = frozenset(round(x * 0.5, 1) for x in range(2, 11))  # 1.0..5.0


@dataclass(frozen=True)
class ValidationFailure:
    code: str
    field: str
    message: str

    def as_dict(self) -> dict[str, str]:
        return {"code": self.code, "field": self.field, "message": self.message}


def validate_business_event(event: dict[str, Any]) -> list[ValidationFailure]:
    """Return an empty list if `event` is valid; otherwise a list of failures.

    `event` is the *normalized* BusinessEvent payload, not the raw Yelp body.
    """
    failures: list[ValidationFailure] = []

    bid = event.get("business_id")
    if not isinstance(bid, str) or not bid.strip():
        failures.append(
            ValidationFailure(
                code="missing_business_id",
                field="business_id",
                message="business_id must be a non-empty string",
            )
        )

    name = event.get("name")
    if not isinstance(name, str) or not name.strip():
        failures.append(
            ValidationFailure(
                code="missing_name",
                field="name",
                message="name must be a non-empty string",
            )
        )

    rating = event.get("rating")
    if rating is None:
        failures.append(
            ValidationFailure(
                code="missing_rating",
                field="rating",
                message="rating is required",
            )
        )
    elif not isinstance(rating, (int, float)):
        failures.append(
            ValidationFailure(
                code="bad_rating_type",
                field="rating",
                message=f"rating must be numeric, got {type(rating).__name__}",
            )
        )
    elif round(float(rating), 1) not in _VALID_RATINGS:
        failures.append(
            ValidationFailure(
                code="rating_out_of_range",
                field="rating",
                message=f"rating={rating!r} outside 1.0..5.0 in 0.5 increments",
            )
        )

    review_count = event.get("review_count")
    if review_count is None:
        failures.append(
            ValidationFailure(
                code="missing_review_count",
                field="review_count",
                message="review_count is required",
            )
        )
    elif not isinstance(review_count, int) or isinstance(review_count, bool):
        failures.append(
            ValidationFailure(
                code="bad_review_count_type",
                field="review_count",
                message="review_count must be an integer",
            )
        )
    elif review_count < 0:
        failures.append(
            ValidationFailure(
                code="negative_review_count",
                field="review_count",
                message=f"review_count={review_count} is negative",
            )
        )

    coords = event.get("coordinates") or {}
    lat = coords.get("latitude")
    lng = coords.get("longitude")
    if lat is None or lng is None:
        failures.append(
            ValidationFailure(
                code="missing_coordinates",
                field="coordinates",
                message="coordinates.latitude and coordinates.longitude required",
            )
        )
    else:
        if not isinstance(lat, (int, float)) or not -90.0 <= float(lat) <= 90.0:
            failures.append(
                ValidationFailure(
                    code="latitude_out_of_range",
                    field="coordinates.latitude",
                    message=f"latitude={lat!r} not in [-90, 90]",
                )
            )
        if not isinstance(lng, (int, float)) or not -180.0 <= float(lng) <= 180.0:
            failures.append(
                ValidationFailure(
                    code="longitude_out_of_range",
                    field="coordinates.longitude",
                    message=f"longitude={lng!r} not in [-180, 180]",
                )
            )

    city = (event.get("location") or {}).get("city")
    if not isinstance(city, str) or not city.strip():
        failures.append(
            ValidationFailure(
                code="missing_city",
                field="location.city",
                message="location.city must be a non-empty string",
            )
        )

    categories = event.get("categories")
    if not isinstance(categories, list) or not categories:
        failures.append(
            ValidationFailure(
                code="missing_categories",
                field="categories",
                message="categories must be a non-empty list",
            )
        )
    else:
        for i, cat in enumerate(categories):
            if not isinstance(cat, str) or not cat.strip():
                failures.append(
                    ValidationFailure(
                        code="bad_category_value",
                        field=f"categories[{i}]",
                        message="each category must be a non-empty string",
                    )
                )

    return failures


def summarize_failures(failures: Iterable[ValidationFailure]) -> str:
    """Compact one-line summary for logs."""
    return ",".join(f"{f.code}:{f.field}" for f in failures)
