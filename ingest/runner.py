"""Polling ingestion runner.

The runner is the glue between `YelpClient` (source) and `EventProducer`
(sink). It does three things:

  1. Iterates configured (term, city) combinations.
  2. For each combination, pages the Yelp search endpoint up to a cap.
  3. Normalizes each business and publishes it.

It exposes a single-sweep mode (`run_once`) and a forever-loop mode
(`run_forever`) that sleeps between sweeps. Graceful shutdown on SIGINT and
SIGTERM is handled by the entrypoint.
"""

from __future__ import annotations

import datetime as dt
import logging
import signal
import time
from collections.abc import Iterable
from dataclasses import dataclass, field

from config.settings import Settings
from ingest.producer import EventProducer
from ingest.schemas import BusinessEvent
from ingest.yelp_client import YelpAPIError, YelpClient

log = logging.getLogger(__name__)


@dataclass
class SweepStats:
    started_at: dt.datetime
    finished_at: dt.datetime | None = None
    cities_attempted: int = 0
    pages_fetched: int = 0
    businesses_seen: int = 0
    accepted: int = 0
    rejected: int = 0
    # Records the broker did not acknowledge before delivery.timeout.ms.
    # Not re-enqueued at the app layer, so a non-zero value is a real loss.
    delivery_failed: int = 0
    errors: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, object]:
        return {
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "cities_attempted": self.cities_attempted,
            "pages_fetched": self.pages_fetched,
            "businesses_seen": self.businesses_seen,
            "accepted": self.accepted,
            "rejected": self.rejected,
            "delivery_failed": self.delivery_failed,
            "errors": self.errors,
        }


class Runner:
    def __init__(
        self,
        settings: Settings,
        *,
        client: YelpClient | None = None,
        producer: EventProducer | None = None,
    ) -> None:
        self._settings = settings
        self._client = client or YelpClient(
            api_key=settings.yelp_api_key,
            base_url=settings.yelp_base_url,
            timeout=settings.yelp_request_timeout_sec,
            api_call_budget=settings.api_call_budget,
        )
        self._producer = producer or EventProducer(settings)
        self._stop = False

    # ----- lifecycle --------------------------------------------------------

    def request_stop(self) -> None:
        log.info("runner.stop_requested")
        self._stop = True

    def install_signal_handlers(self) -> None:
        signal.signal(signal.SIGINT, lambda *_: self.request_stop())
        signal.signal(signal.SIGTERM, lambda *_: self.request_stop())

    def close(self) -> None:
        try:
            self._producer.close()
        finally:
            self._client.close()

    def inject_bad_local(self) -> None:
        """Publish one invalid record to the producer-side Kafka DLQ topic."""
        self._producer.inject_bad_for_test()
        self._producer.flush()

    def inject_bad_raw(self) -> None:
        """Publish one malformed envelope to the raw topic so the Databricks
        Silver job quarantines it into the Delta DLQ table."""
        self._producer.inject_bad_raw()
        self._producer.flush()

    # ----- driving ----------------------------------------------------------

    def run_once(
        self,
        *,
        cities: Iterable[str] | None = None,
        terms: Iterable[str] | None = None,
        max_pages_per_city: int | None = None,
    ) -> SweepStats:
        stats = SweepStats(started_at=dt.datetime.now(dt.UTC))
        s = self._settings
        cities = list(cities) if cities is not None else list(s.cities)
        terms = list(terms) if terms is not None else list(s.terms)
        max_pages = (
            max_pages_per_city
            if max_pages_per_city is not None
            else s.max_pages_per_city
        )

        log.info(
            "sweep.start cities=%d terms=%d page_size=%d max_pages=%d",
            len(cities), len(terms), s.page_size, max_pages,
        )

        for city in cities:
            if self._stop:
                break
            stats.cities_attempted += 1
            for term in terms:
                if self._stop:
                    break
                try:
                    self._sweep_city_term(
                        city=city,
                        term=term,
                        max_pages=max_pages,
                        stats=stats,
                    )
                except YelpAPIError as exc:
                    msg = f"city={city!r} term={term!r} status={exc.status}"
                    log.error("sweep.api_error %s", msg)
                    stats.errors.append(msg)
                except Exception as exc:
                    msg = f"city={city!r} term={term!r} err={exc!r}"
                    log.exception("sweep.unexpected_error %s", msg)
                    stats.errors.append(msg)

        # Block until pending records are durably delivered.
        self._producer.flush()
        stats.delivery_failed = self._producer.failed
        stats.finished_at = dt.datetime.now(dt.UTC)

        if stats.delivery_failed:
            log.error(
                "sweep.delivery_failures count=%d (records not re-enqueued)",
                stats.delivery_failed,
            )
        log.info("sweep.done %s", stats.as_dict())
        return stats

    def run_forever(self) -> None:
        s = self._settings
        while not self._stop:
            cycle_start = time.monotonic()
            self.run_once()
            elapsed = time.monotonic() - cycle_start
            sleep_for = max(0.0, s.poll_interval_sec - elapsed)
            if self._stop:
                break
            log.info("runner.sleep seconds=%.1f", sleep_for)
            # Sleep in small slices so SIGINT is responsive.
            slept = 0.0
            while slept < sleep_for and not self._stop:
                step = min(1.0, sleep_for - slept)
                time.sleep(step)
                slept += step

    # ----- internals --------------------------------------------------------

    def _sweep_city_term(
        self,
        *,
        city: str,
        term: str,
        max_pages: int,
        stats: SweepStats,
    ) -> None:
        s = self._settings
        last_page = -1
        for biz in self._client.iter_search(
            location=city,
            term=term,
            page_size=s.page_size,
            max_pages=max_pages,
        ):
            # Estimate current page from offset so we can count distinct pages.
            page = self._client.calls_made - 1
            if page != last_page:
                stats.pages_fetched += 1
                last_page = page

            stats.businesses_seen += 1
            fetched_at = dt.datetime.now(dt.UTC)
            try:
                event = BusinessEvent.from_yelp(
                    biz, search_term=term, search_location=city
                )
            except Exception as exc:
                # Coercion failed - treat as a pre-validation failure and DLQ.
                log.warning(
                    "normalize.failed business_id=%s err=%r",
                    biz.get("id"), exc,
                )
                self._producer.quarantine(
                    biz, fetched_at=fetched_at, reason="normalize_failed"
                )
                stats.rejected += 1
                continue

            accepted = self._producer.publish_business(event, fetched_at=fetched_at)
            if accepted:
                stats.accepted += 1
            else:
                stats.rejected += 1
