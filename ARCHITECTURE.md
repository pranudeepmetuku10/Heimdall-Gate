# Heimdall-Gate architecture

This document is the technical reference for the pipeline: what each component
is responsible for, the contracts between them, and the correctness properties
the design buys us. For setup steps see [DATABRICKS_SETUP.md](DATABRICKS_SETUP.md);
for a visual overview open [docs/architecture.html](docs/architecture.html) in a
browser.

The guiding principle is **a single responsibility per stage and an explicit
contract at every boundary**. Nothing downstream reaches back upstream: Spark
never calls Yelp, Gold never reads Bronze, and validation rules have exactly one
source of truth.

---

## 1. Component map

| Stage | Component | Responsibility | Does NOT do |
|-------|-----------|----------------|-------------|
| Source | Yelp Fusion API | System of record for business listings | — |
| Ingest | `ingest/` Python service | Transport: poll, page, retry, normalize, build envelope, publish | No analytics, no state |
| Transport | Kafka topic `heimdall.raw.business` | Durable buffer, replay log, decoupling point | No transformation |
| Land | `01_bronze_ingest` | Append raw envelopes verbatim to Delta | No parsing, no validation |
| Conform | `02_silver_transform` | Parse, validate, dedup, MERGE valid / quarantine bad | No aggregation |
| Serve | `03_gold_aggregates` | Daily rating drift and category trend aggregates | No row-level data |

Each row in that "Does NOT do" column is a deliberate boundary. They are what
keep the system debuggable: when a number looks wrong you know which stage owns
it.

---

## 2. End-to-end data flow

```
Yelp Fusion API
   │  HTTPS, Bearer auth, paged (limit<=50, offset<=1000)
   ▼
ingest.runner ──► ingest.producer ──► Kafka: heimdall.raw.business   (key = business_id)
   │ normalize          │ validate          │
   │ (BusinessEvent)    │ (shared rules)     │
   │                    └─ on failure ─────► Kafka: heimdall.dlq.ingest
   ▼
[ Databricks, serverless, trigger(availableNow=True) ]
   │
   ├─ 01 Bronze ──► heimdall.bronze.business_events_raw   (append-only, raw STRING envelope)
   │
   ├─ 02 Silver ──► parse + validate
   │                 ├─ valid ──► MERGE ──► heimdall.silver.business_events
   │                 └─ invalid ─────────► heimdall.dlq.business   (reason + codes + payload)
   │
   └─ 03 Gold  ──► heimdall.gold.rating_drift_daily
                   heimdall.gold.category_trends_daily
```

The two split points — the producer-side DLQ and the Silver-side DLQ — are why
nothing is silently dropped. A malformed record always lands *somewhere* you can
query, tagged with why it failed.

---

## 3. The boundary contract: the event envelope

Everything on `heimdall.raw.business` is one JSON envelope. This is the contract
between the Python world and the Spark world. It is defined once in
[`ingest/schemas.py`](ingest/schemas.py) and mirrored as a Spark `StructType` in
[`02_silver_transform.py`](databricks/jobs/02_silver_transform.py).

```json
{
  "event_id":        "f1c8...-uuid4",
  "producer_id":     "heimdall-ingest-local",
  "source":          "yelp",
  "source_endpoint": "businesses/search",
  "schema_version":  1,
  "observed_at":     "2026-06-09T12:00:00.000+00:00",
  "ingest_lag_ms":   142,
  "payload": {
    "business_id":   "WavvLdfdP6g8aZTtbBQHTw",
    "name":          "Cafe Constant",
    "rating":        4.5,
    "review_count":  218,
    "categories":    ["cafes"],
    "coordinates":   { "latitude": 42.36, "longitude": -71.06 },
    "location":      { "city": "Boston", "state": "MA", "country": "US", "zip_code": "02108" },
    "search_term":   "restaurants",
    "search_location": "Boston"
  }
}
```

Contract rules that the rest of the system relies on:

- **`business_id` is the Kafka partition key.** All events for one business land
  on the same partition, so per-business ordering is preserved through the log.
- **`observed_at` is producer wall-clock at fetch time, always UTC.** It is the
  observation timestamp that Gold buckets by day — never processing time. It is
  *not* a dedup key (each fetch mints a fresh value by design); see §5.
- **`event_id` is a per-envelope UUID, stable through Bronze.** It is Silver's
  MERGE key, so reprocessing the same Bronze rows is idempotent.
- **`schema_version` is explicit.** A breaking change bumps it; consumers can
  branch on it during a migration window instead of guessing.
- **`ingest_lag_ms`** is fetch-to-publish latency, carried for observability so
  you can see producer health without external metrics.

### Why JSON-in-a-string in Bronze, not a typed column

Bronze stores the entire envelope as one `STRING` column (`raw_envelope`) and
decodes it lazily in Silver. The payoff: when Yelp adds a field, the producer and
Bronze do not change at all, and Silver picks it up only if and when we extend
the `StructType`. Schema drift cannot break ingestion. The cost is one parse per
row in Silver, which is negligible at this volume. A schema registry + Avro is
the scale-up path (see §9).

---

## 4. Table schemas (the medallion)

### Bronze — `heimdall.bronze.business_events_raw`
Append-only landing zone. Partitioned by `kafka_topic`. One row per Kafka message.

| Column | Type | Note |
|--------|------|------|
| `ingest_ts` | TIMESTAMP | When Spark wrote the row |
| `kafka_topic` / `kafka_partition` / `kafka_offset` | STRING/INT/BIGINT | Lineage back to the log |
| `kafka_key` | STRING | `business_id` |
| `raw_envelope` | STRING | Verbatim JSON envelope |

### Silver — `heimdall.silver.business_events`
Validated, typed observation time series: one row per source event, MERGEd on
`event_id`. Not physically partitioned — `city` was rejected as a partition
column (unbounded free text, and the `event_id` MERGE key cannot prune it, so it
only produced small files); cluster on read-hot columns once volume justifies
it.

Typed columns: `business_id, name, rating, review_count, price, is_closed, url,
categories, latitude, longitude, city, state, country, zip_code, search_term,
search_location, observed_at, ingest_ts, event_id, producer_id, schema_version`.

### DLQ — `heimdall.dlq.business`
Quarantine. Partitioned by `failure_reason`. `dlq_key` is a content hash
(`kafka_topic, kafka_partition, kafka_offset, raw_envelope`) used as the MERGE
key so a retried micro-batch cannot double-count. Carries `failure_codes
ARRAY<STRING>` and the original `raw_envelope` so a bad record can be diagnosed
and replayed without re-hitting the API.

### Gold — `heimdall.gold.rating_drift_daily`
Grain: `(day, business_id)`. `rating_avg/min/max`, `review_count_max`,
`observations`.

### Gold — `heimdall.gold.category_trends_daily`
Grain: `(day, category, city)`. `total_observations`, `distinct_businesses`,
`avg_rating`.

---

## 5. Processing semantics

### Why `trigger(availableNow=True)` and not continuous streaming

Databricks Free Edition runs on serverless compute that is not designed to host
a 24/7 streaming query — there is no always-on cluster to pin one to.
`availableNow` is the correct primitive for this constraint, not a workaround:

- Each run reads **all currently available** offsets/files, processes them in
  one or more micro-batches, commits the checkpoint, and **exits**.
- A Databricks Workflow schedule (every 5–30 min) re-fires the job. Between runs
  there is no idle compute and no cost.
- Because it uses the **same Structured Streaming engine** as a continuous query
  — same checkpoint, same offset log, same exactly-once sink protocol — you get
  streaming correctness with batch economics. Switching to continuous later is a
  one-line trigger change, no rewrite.

This is the single most important design point for the talk: *the serverless
constraint did not force a downgrade to batch; it pointed at the right trigger.*

### Checkpoints and offset management

Every streaming job owns a checkpoint directory on a Unity Catalog volume.
The checkpoint holds the source offsets (Kafka offsets / Auto Loader file list)
and the sink commit log. On restart a job resumes exactly where it left off;
`startingOffsets=earliest` only applies the first time a checkpoint is created.

### Delivery guarantees, end to end

| Hop | Guarantee | Mechanism |
|-----|-----------|-----------|
| Producer → Kafka | At-least-once, no duplicates within a session | Idempotent producer (`enable.idempotence=true`, `acks=all`) |
| Kafka → Bronze | Exactly-once for retained offsets | Structured Streaming checkpoint + Delta atomic commit; `failOnDataLoss=true` so a retention gap fails loudly rather than skipping |
| Bronze → Silver | Effectively-once at rest | MERGE on `event_id` (frozen in Bronze) is a re-load guard: reprocessing updates in place instead of inserting |
| Silver → Gold | Idempotent recompute | `replaceWhere` overwrites only the bounded day partitions; an empty window is a no-op, not a delete |

The honest framing: the wire path is **at-least-once**, and the **idempotent
MERGE on `event_id`** makes the *table state* effectively-once under
reprocessing. We do not claim end-to-end exactly-once. Note the boundary of the
claim: a producer restart that re-fetches a business publishes a *new* event
(new `event_id`, new `observed_at`), which is a legitimate new observation for a
time series, not a duplicate to collapse. Silver is an observation stream, not a
current-state table.

### Silver dedup detail

The only duplicate Silver collapses is the *same envelope* appearing twice — an
at-least-once redelivery into Bronze, or a checkpoint-reset reprocess. Those rows
are byte-identical and share an `event_id`, so the MERGE updates in place. Within
a micro-batch, a `row_number()` window partitioned by `event_id` (ordered by
`ingest_ts` then `kafka_offset`, deterministic) keeps one row before the MERGE,
because Delta rejects a MERGE whose source has two rows matching one target key.

### Gold recompute window

Gold is batch, not streaming. It recomputes only the last `recompute_window_days`
(default 2) of daily partitions and overwrites them via `replaceWhere`, so
late-arriving Silver rows are reflected without rewriting history. The window
boundary is computed **once in the driver as a date literal** and used for both
the read filter and the `replaceWhere` predicate — see the comment in
[`03_gold_aggregates.py`](databricks/jobs/03_gold_aggregates.py) for why a
naive `current_date()` here is a midnight-boundary bug.

---

## 6. Validation and quarantine

Validation rules live in exactly one place,
[`validation/validators.py`](validation/validators.py), and are applied twice:

1. **Producer-side** (Python), before publish. Failures → `heimdall.dlq.ingest`.
2. **Silver-side** (PySpark predicates), re-implemented to run row-parallel.
   Failures → `heimdall.dlq.business`.

The PySpark version is a translation, not a second design — the Silver job
header explicitly notes that a rule change in the Python module must be mirrored.
Keeping the canonical list in one file makes any drift visible in code review.

Because the producer applies the same rules before publishing, the Silver DLQ
normally fills only on genuine cross-layer drift (defense in depth) or from a
record injected straight onto the raw topic. Silver-side rules cover: a
parseable envelope, non-null `event_id` and `observed_at` (the MERGE key and the
Gold time bucket), non-empty `business_id`/`name`/`city`, `rating` in 1.0–5.0 in
0.5 steps, non-negative integer `review_count`, coordinates in valid lat/long
ranges, and a non-empty `categories` list. A row whose envelope will not parse
is quarantined with reason `envelope_parse_failed` rather than dropped, so the
"nothing is dropped" guarantee holds literally. Every failure carries a stable
`code` (e.g. `rating_out_of_range`) so the DLQ is groupable:

```sql
SELECT code, COUNT(*) AS n
FROM heimdall.dlq.business
LATERAL VIEW explode(failure_codes) t AS code
GROUP BY code ORDER BY n DESC;
```

That query is the operational pulse of data quality, and it is one of the demo
queries for the talk.

---

## 7. Failure handling and operability

- **API errors.** 4xx (other than 429) raise immediately — retrying a 400 will
  not fix it. 429 and 5xx are retried with exponential backoff (tenacity, capped
  at 4 attempts). Transport errors are treated as retryable.
- **Quota guardrail.** The Yelp free tier is 5,000 calls/day. The client
  enforces an in-process `api_call_budget` ceiling and stops paging when hit, so
  a runaway loop cannot drain the quota.
- **Producer backpressure.** A full local producer queue triggers a flush-and-
  retry rather than a drop; delivery is confirmed via callback and counted.
- **Structured logs.** JSON or console via `HEIMDALL_LOG_FORMAT`; every sweep
  emits a `SweepStats` record (seen / accepted / rejected / errors) so a run is
  auditable from logs alone.
- **Replay.** Because Kafka retains the raw log and Bronze is append-only, any
  downstream table can be rebuilt by clearing its checkpoint and reprocessing —
  no need to re-call Yelp.

---

## 8. Why this design holds up (the talk's thesis)

1. **The constraint shaped the architecture, it did not compromise it.**
   Serverless + ephemeral compute → `availableNow` micro-batches with full
   streaming semantics. Same correctness, no idle cost.
2. **Decoupling via Kafka** lets the pull cadence, processing cadence, and
   replay all move independently. The API's 5k/day cap never reaches Spark.
3. **One source of truth for validation**, applied at two layers, with a stated
   policy for keeping them in sync.
4. **Idempotency at rest** (`MERGE`, `replaceWhere`) gives correct tables under
   at-least-once delivery without overclaiming exactly-once.
5. **Schema drift is survivable** because Bronze is opaque and Silver decodes
   lazily.
6. **Nothing is dropped.** Two quarantine sinks, both queryable, both carrying
   the original payload and a stable failure code.

---

## 9. Known limits and the scale-up path

| Today | At scale | Why deferred |
|-------|----------|--------------|
| Pydantic envelope, JSON on the wire | Confluent Schema Registry + Avro | Registry is another service to operate; not worth it for a demo |
| Polling Yelp | Webhook/event ingestion | Yelp has no push API; producer contract is already event-shaped |
| Validation mirrored by hand in two languages | Single rule engine (e.g. compiled to both) or Great Expectations on Silver | Hand-mirroring is honest and visible at this size |
| Cron-style `availableNow` re-fire | Lakeflow / DLT pipelines | Free Edition scheduling is sufficient for the demo |
| Single source (Yelp) | Multi-source fan-in (OpenStreetMap Overpass, OpenAQ) | One source proves the pattern |

These are written as a roadmap, not apologies — each line states the trigger
that would justify the extra complexity.

---

## 10. Yelp Terms of Use posture

Yelp's API ToU restricts long-term caching of business content and requires
attribution. Heimdall-Gate retains only the fields needed for trend analysis
(identity, coordinates, rating, review count, category slugs, timestamps) — no
review text, no photos. For any real deployment, add a TTL job that prunes rows
older than 24h, or swap the source connector to an unrestricted feed. This is
called out so the demo is not mistaken for a redistribution system.
