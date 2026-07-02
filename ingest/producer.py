"""Kafka producer wrapper.

Wraps `confluent_kafka.Producer` with the project's envelope contract and
producer-side DLQ handling.

Two reasons for the wrapper:

  1. Centralize serialization. The producer always emits orjson-encoded
     envelopes with the business_id as the partition key. No call site decides
     the wire format.

  2. Centralize the DLQ split. A record that fails pre-publish validation is
     written to the DLQ topic instead of being silently dropped. Failures that
     happen after the broker accepts the message (delivery errors) are logged
     and bubble up via the delivery callback.

The producer can also mirror outbound records to a local newline-delimited JSON
file when `file_sink_enabled=True`. This is the "volume-source" mode used to
demo the pipeline on Databricks Free Edition without a cloud Kafka.
"""

from __future__ import annotations

import datetime as dt
import logging
import os
import pathlib
import threading
from collections.abc import Callable
from typing import Any

import orjson
from confluent_kafka import KafkaError, KafkaException, Producer

from config.settings import Settings
from ingest.schemas import BusinessEvent, Envelope
from validation.validators import (
    ValidationFailure,
    summarize_failures,
    validate_business_event,
)

log = logging.getLogger(__name__)


def _build_producer_config(settings: Settings) -> dict[str, Any]:
    cfg: dict[str, Any] = {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "client.id": settings.producer_id,
        "linger.ms": settings.kafka_linger_ms,
        "batch.size": settings.kafka_batch_size,
        "compression.type": settings.kafka_compression_type,
        "acks": settings.kafka_acks,
        "enable.idempotence": settings.kafka_enable_idempotence,
        "max.in.flight.requests.per.connection": settings.kafka_max_in_flight,
        # Avoid surprising callers if the broker is briefly unreachable.
        "message.timeout.ms": 30_000,
        "delivery.timeout.ms": 30_000,
        "retries": 10,
    }
    if settings.kafka_security_protocol != "PLAINTEXT":
        cfg["security.protocol"] = settings.kafka_security_protocol
    if settings.kafka_security_protocol.startswith("SASL_"):
        cfg["sasl.mechanism"] = settings.kafka_sasl_mechanism
        cfg["sasl.username"] = settings.kafka_sasl_username
        cfg["sasl.password"] = settings.kafka_sasl_password
    return cfg


class EventProducer:
    """High-level facade over confluent_kafka.Producer."""

    def __init__(
        self,
        settings: Settings,
        *,
        producer_factory: Callable[[dict[str, Any]], Producer] = Producer,
        clock: Callable[[], dt.datetime] = lambda: dt.datetime.now(dt.UTC),
    ) -> None:
        self._settings = settings
        self._clock = clock
        self._delivered = 0
        self._failed = 0
        self._lock = threading.Lock()

        if settings.kafka_enabled:
            self._producer: Producer | None = producer_factory(
                _build_producer_config(settings)
            )
        else:
            self._producer = None
            log.warning("kafka.disabled bootstrap_servers is empty; running file-sink only")

        self._file_sink: pathlib.Path | None = None
        if settings.file_sink_enabled:
            sink = pathlib.Path(settings.file_sink_path)
            sink.mkdir(parents=True, exist_ok=True)
            ts = self._clock().strftime("%Y%m%dT%H%M%SZ")
            self._file_sink = sink / f"events-{ts}-{os.getpid()}.jsonl"
            log.info("file_sink.enabled path=%s", self._file_sink)

    # ------------------------------------------------------------------ stats

    @property
    def delivered(self) -> int:
        return self._delivered

    @property
    def failed(self) -> int:
        return self._failed

    # ----------------------------------------------------------------- public

    def publish_business(
        self,
        business: BusinessEvent,
        *,
        fetched_at: dt.datetime,
    ) -> bool:
        """Validate and publish one normalized business event.

        Returns True on accepted-for-send, False if it was diverted to DLQ.
        Acceptance does not mean delivered; check `flush()` for that.
        """
        # The validator works on plain dicts. Use mode="json" to coerce known
        # types (e.g. enums) consistently with what lands on the wire.
        as_dict = business.model_dump(mode="json")
        failures = validate_business_event(as_dict)
        if failures:
            self._publish_dlq(
                payload=as_dict,
                failures=failures,
                fetched_at=fetched_at,
                reason="pre_publish_validation",
            )
            return False

        envelope = Envelope(
            producer_id=self._settings.producer_id,
            observed_at=fetched_at,
            ingest_lag_ms=max(
                0, int((self._clock() - fetched_at).total_seconds() * 1000)
            ),
            payload=business,
        )
        self._publish_raw(
            topic=self._settings.kafka_topic_raw,
            key=envelope.kafka_key(),
            value=envelope.model_dump(mode="json"),
        )
        return True

    def quarantine(
        self,
        payload: dict[str, Any],
        *,
        fetched_at: dt.datetime,
        reason: str,
        failures: list[ValidationFailure] | None = None,
    ) -> None:
        """Route a record to the producer-side DLQ topic.

        Used for payloads that cannot even be normalized into a BusinessEvent.
        """
        self._publish_dlq(
            payload=payload,
            failures=failures or [],
            fetched_at=fetched_at,
            reason=reason,
        )

    def inject_bad_for_test(self) -> None:
        """Publish an invalid record to the producer-side DLQ topic
        (``heimdall.dlq.ingest``). This is the LOCAL data-quality path and is
        independent of the Databricks Delta DLQ table. Used by the smoke test.
        """
        bad = {"business_id": "", "name": "", "rating": 99, "review_count": -1}
        self._publish_dlq(
            payload=bad,
            failures=validate_business_event(bad),
            fetched_at=self._clock(),
            reason="injected_for_test",
        )

    def inject_bad_raw(self) -> None:
        """Publish a structurally-valid but business-invalid envelope to the
        RAW topic (``heimdall.raw.business``), bypassing pre-publish validation.

        Unlike ``inject_bad_for_test``, this record flows through Bronze and is
        quarantined by the Silver job into the Delta ``dlq.business`` table, so
        it is the right way to demonstrate the Databricks quarantine path.
        """
        bad = BusinessEvent(
            business_id="demo-quarantine-0001",
            name="Heimdall Quarantine Demo",
            rating=9.9,          # out of 1.0..5.0 -> rating_out_of_range
            review_count=-1,     # negative       -> negative_review_count
            categories=["restaurants"],
            coordinates={"latitude": 42.3601, "longitude": -71.0589},
            location={"city": "Boston", "state": "MA", "country": "US"},
            search_term="restaurants",
            search_location="Boston",
        )
        now = self._clock()
        envelope = Envelope(
            producer_id=self._settings.producer_id,
            observed_at=now,
            payload=bad,
        )
        log.warning("inject_bad_raw publishing malformed envelope to raw topic")
        self._publish_raw(
            topic=self._settings.kafka_topic_raw,
            key=envelope.kafka_key(),
            value=envelope.model_dump(mode="json"),
        )

    def flush(self, timeout: float = 15.0) -> int:
        """Block until outstanding records are delivered or the timeout
        elapses. Returns the number of messages still in flight (0 means
        clean shutdown)."""
        if self._producer is None:
            return 0
        remaining = self._producer.flush(timeout)
        if remaining:
            log.warning("kafka.flush_incomplete remaining=%d", remaining)
        return remaining

    def close(self) -> None:
        try:
            self.flush()
        finally:
            self._producer = None

    # ---------------------------------------------------------------- private

    def _publish_raw(
        self,
        *,
        topic: str,
        key: str,
        value: dict[str, Any],
    ) -> None:
        encoded = orjson.dumps(value)
        if self._file_sink is not None:
            # Mirror to disk regardless of Kafka availability.
            with self._file_sink.open("ab") as f:
                f.write(encoded)
                f.write(b"\n")
        if self._producer is None:
            return
        try:
            self._producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=encoded,
                on_delivery=self._on_delivery,
            )
        except BufferError:
            # Producer queue is full; flush and retry once.
            log.warning("kafka.produce_buffer_full; flushing then retrying")
            self._producer.poll(1.0)
            self._producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=encoded,
                on_delivery=self._on_delivery,
            )
        # Drain delivery callbacks without blocking.
        self._producer.poll(0)

    def _publish_dlq(
        self,
        *,
        payload: dict[str, Any],
        failures: list[ValidationFailure],
        fetched_at: dt.datetime,
        reason: str,
    ) -> None:
        log.warning(
            "validation.failed reason=%s codes=%s",
            reason,
            summarize_failures(failures),
        )
        dlq_value = {
            "producer_id": self._settings.producer_id,
            "reason": reason,
            "failures": [f.as_dict() for f in failures],
            "observed_at": fetched_at.astimezone(dt.UTC).isoformat(),
            "payload": payload,
        }
        key = payload.get("business_id") or "unknown"
        self._publish_raw(
            topic=self._settings.kafka_topic_dlq,
            key=str(key),
            value=dlq_value,
        )

    def _on_delivery(self, err: KafkaError | None, msg: Any) -> None:
        with self._lock:
            if err is not None:
                self._failed += 1
                log.error(
                    "kafka.delivery_failed topic=%s key=%s err=%s",
                    msg.topic() if msg else "?",
                    (msg.key() or b"").decode("utf-8", "replace") if msg else "?",
                    err,
                )
                return
            self._delivered += 1
            if self._delivered % 100 == 0:
                log.info(
                    "kafka.delivery_progress delivered=%d failed=%d",
                    self._delivered,
                    self._failed,
                )


__all__ = ["EventProducer", "KafkaException"]
