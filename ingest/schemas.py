"""Envelope and payload models for events published to Kafka.

The envelope is what lands in the `heimdall.raw.business` topic. It carries the
normalized business payload plus metadata used by downstream Spark jobs for
ordering, deduplication, and lineage.

Downstream contract:

  - `event_id`            UUIDv4, unique per envelope
  - `producer_id`         identifies the publisher process
  - `source`              external system the event came from (e.g. "yelp")
  - `source_endpoint`     specific endpoint used (e.g. "businesses/search")
  - `observed_at`         when the producer fetched the data (UTC, ISO-8601)
  - `ingest_lag_ms`       wall-clock latency between API call and publish
  - `schema_version`      monotonically incremented on breaking changes
  - `payload`             normalized BusinessEvent
"""

from __future__ import annotations

import datetime as dt
import uuid
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


SCHEMA_VERSION = 1


class Coordinates(BaseModel):
    model_config = ConfigDict(extra="ignore")
    latitude: float | None = None
    longitude: float | None = None


class Location(BaseModel):
    model_config = ConfigDict(extra="ignore")
    address1: str | None = None
    city: str | None = None
    state: str | None = None
    country: str | None = None
    zip_code: str | None = None


class BusinessEvent(BaseModel):
    """Normalized business record.

    This is intentionally a subset of the Yelp business object - we only carry
    fields we will use in Silver/Gold and that Yelp's ToU permits redistributing
    in derived analytics.
    """

    model_config = ConfigDict(extra="ignore")

    business_id: str
    name: str
    url: str | None = None
    image_url: str | None = None
    is_closed: bool | None = None
    rating: float | None = None
    review_count: int | None = None
    price: str | None = None
    phone: str | None = None
    categories: list[str] = Field(default_factory=list)
    coordinates: Coordinates = Field(default_factory=Coordinates)
    location: Location = Field(default_factory=Location)
    transactions: list[str] = Field(default_factory=list)

    # Search-call context (which sweep produced this record).
    search_term: str
    search_location: str

    @classmethod
    def from_yelp(
        cls,
        body: dict[str, Any],
        *,
        search_term: str,
        search_location: str,
    ) -> "BusinessEvent":
        """Build a BusinessEvent from a Yelp `/businesses/search` element.

        Yelp's category objects look like `{"alias": "...", "title": "..."}`.
        We persist the alias because it is the stable identifier.
        """
        cats = body.get("categories") or []
        alias_list = [c.get("alias") for c in cats if c.get("alias")]

        return cls(
            business_id=body["id"],
            name=body.get("name", ""),
            url=body.get("url"),
            image_url=body.get("image_url"),
            is_closed=body.get("is_closed"),
            rating=body.get("rating"),
            review_count=body.get("review_count"),
            price=body.get("price"),
            phone=body.get("phone"),
            categories=alias_list,
            coordinates=Coordinates(**(body.get("coordinates") or {})),
            location=Location(**(body.get("location") or {})),
            transactions=list(body.get("transactions") or []),
            search_term=search_term,
            search_location=search_location,
        )


class Envelope(BaseModel):
    """Top-level message published to Kafka."""

    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    producer_id: str
    source: str = "yelp"
    source_endpoint: str = "businesses/search"
    schema_version: int = SCHEMA_VERSION
    observed_at: dt.datetime
    ingest_lag_ms: int = 0
    payload: BusinessEvent

    @field_validator("observed_at")
    @classmethod
    def _ensure_utc(cls, v: dt.datetime) -> dt.datetime:
        if v.tzinfo is None:
            return v.replace(tzinfo=dt.timezone.utc)
        return v.astimezone(dt.timezone.utc)

    def kafka_key(self) -> str:
        """Partition key. Co-locates events for the same business on one
        partition so downstream consumers can reason about ordering per-business.
        """
        return self.payload.business_id
