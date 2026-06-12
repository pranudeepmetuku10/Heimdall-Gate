# Heimdall-Gate

A streaming gateway that ingests business listing events from the Yelp Fusion API,
publishes them to Kafka, and processes them with Spark Structured Streaming on
Databricks into a Bronze / Silver / Gold Delta Lake medallion.

The pipeline validates records at the edge, quarantines malformed or suspicious
events, and produces analytics-ready tables describing rating drift, review-count
deltas, and category trends across cities.

This is a personal engineering project. It is not affiliated with Yelp or
Databricks.

**Documentation**

- [ARCHITECTURE.md](ARCHITECTURE.md) — the technical reference: component
  contracts, table schemas, processing semantics, and delivery guarantees.
- [docs/architecture.html](docs/architecture.html) — a self-contained visual
  walkthrough of the flow, the medallion, and the technology at each layer.
  Open it in a browser; no build step or network access required.
- [DATABRICKS_SETUP.md](DATABRICKS_SETUP.md) — step-by-step Databricks Free
  Edition setup for a working demo.

---

## Architecture

```
                  +----------------------+
                  |   Yelp Fusion API    |
                  +----------+-----------+
                             |
                             | HTTPS (Bearer auth, paged)
                             v
                  +----------------------+
                  |  ingest.runner       |   Python service
                  |  - poll loop         |   confluent-kafka producer
                  |  - normalize         |   pydantic schemas
                  |  - sign envelope     |
                  +----------+-----------+
                             |
                             | Kafka (idempotent producer)
                             v
            +---------------------------------+
            | topic: heimdall.raw.business    |
            | topic: heimdall.dlq.ingest      |
            +----------------+----------------+
                             |
                             | Spark Structured Streaming
                             | trigger(availableNow=True)
                             v
              +-----------------------------+
              | Databricks (Free Edition)   |
              |                             |
              |  bronze.business_events_raw |
              |        |                    |
              |        v                    |
              |  silver.business_events     |
              |        |                    |
              |        +--> dlq.business    |
              |        |                    |
              |        v                    |
              |  gold.rating_drift_daily    |
              |  gold.category_trends_daily |
              +-----------------------------+
```

The pipeline is intentionally split at two boundaries:

1. The Python ingestion service handles transport concerns (rate limits, retries,
   pagination, envelope construction) and emits a frozen record shape onto Kafka.
2. The Spark layer handles state, idempotency, and analytical modeling. It treats
   Kafka as the source of truth and never calls Yelp directly.

---

## Why this shape

- **Kafka as buffer.** Yelp's API has a 5,000 request/day cap on the free tier.
  Buffering raw responses in Kafka decouples the pull cadence from downstream
  processing and lets us replay history without re-hitting the API.
- **Bronze / Silver / Gold.** Bronze is an append-only landing zone with the
  exact envelope we received. Silver is the validated, typed, deduplicated form.
  Gold is the analytical surface. Each hop is idempotent.
- **Available-now triggering.** Databricks Free Edition runs on serverless
  compute that is not designed for continuous 24/7 streaming jobs. We use
  `trigger(availableNow=True)` so each run drains whatever is in Kafka and exits.
  A scheduled Databricks job re-fires the notebook every N minutes.
- **Quarantine over reject.** Records that fail schema or business validation are
  routed to a `dlq` Delta table with the failure reason and the original payload.
  Nothing is silently dropped.

---

## Repository layout

```
heimdall-gate/
  config/                 runtime config (env-driven) and logging
  ingest/                 Python service: API client, producer, runner
  validation/             shared validation rules used by ingest + spark
  databricks/jobs/        Spark Structured Streaming jobs (run as Databricks jobs)
  databricks/notebooks/   Exploratory notebooks
  docs/architecture.html  self-contained visual architecture walkthrough
  scripts/                local helpers (topic bootstrap, smoke test)
  tests/                  pytest unit tests
  docker-compose.yml      local Kafka (KRaft mode, single broker)
  Makefile                common entrypoints
  pyproject.toml          pytest + ruff configuration
  requirements.txt        Python deps (pinned to wheels for CPython 3.11-3.13)
  requirements-dev.txt    test/lint deps
  .env.example            documented environment variables
  ARCHITECTURE.md         technical reference (contracts, schemas, guarantees)
  DATABRICKS_SETUP.md     step-by-step Databricks Free Edition setup
```

---

## Quick start (local Kafka + ingestion)

Prerequisites: Docker, Python 3.11+, a Yelp Fusion API key
(https://docs.developer.yelp.com/docs/fusion-intro).

```bash
# 1. Clone and enter
cd heimdall-gate

# 2. Configure
cp .env.example .env
# Edit .env, set YELP_API_KEY and any city overrides

# 3. Start local Kafka (KRaft, no Zookeeper)
make kafka-up

# 4. Create topics
make topics

# 5. Install Python deps in a venv
make install

# 6. Run a one-shot ingestion (single poll of configured cities)
make ingest-once

# 7. Or run the polling loop (Ctrl+C to stop)
make ingest

# 8. Tail the topic to verify
make consume
```

For Databricks: see [DATABRICKS_SETUP.md](DATABRICKS_SETUP.md).

### Development

```bash
make install-dev   # venv + runtime + test/lint deps
make test          # pytest (no Kafka or network required)
make lint          # ruff
```

The unit tests stub the Yelp API with `respx` and run the producer in
file-sink mode, so they need neither a live API key nor a running broker. The
pinned dependency set installs from prebuilt wheels on CPython 3.11-3.13
(`confluent-kafka` bundles `librdkafka`, so no system C library is required).

---

## Configuration

All configuration is environment-driven. See `.env.example` for the full list.
The notable ones:

| Variable | Default | Purpose |
|----------|---------|---------|
| `YELP_API_KEY` | _required_ | Bearer token for Yelp Fusion |
| `HEIMDALL_CITIES` | `Boston,Seattle,Austin` | Comma-separated locations to poll |
| `HEIMDALL_TERMS` | `restaurants` | Search terms |
| `HEIMDALL_POLL_INTERVAL_SEC` | `900` | Seconds between full sweeps |
| `HEIMDALL_PAGE_SIZE` | `50` | Yelp page size (max 50) |
| `HEIMDALL_MAX_PAGES_PER_CITY` | `4` | Cap on pages per city per sweep |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9094` | Kafka brokers (9094 is the local external listener) |
| `KAFKA_TOPIC_RAW` | `heimdall.raw.business` | Raw events topic |
| `KAFKA_TOPIC_DLQ` | `heimdall.dlq.ingest` | Producer-side DLQ |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | Set to `SASL_SSL` for Confluent Cloud |
| `HEIMDALL_LOG_LEVEL` | `INFO` | Standard Python log levels |
| `HEIMDALL_LOG_FORMAT` | `console` | `json` or `console` |

---

## Known limitations and tradeoffs

**Databricks Free Edition is serverless and ephemeral.** There is no always-on
cluster you can pin a continuous streaming query to. All Spark jobs in
`databricks/jobs/` use `trigger(availableNow=True)` so they drain the source on
each run and exit. The expected operating model is a Databricks scheduled job
firing every 5-30 minutes, not a long-lived streaming application.

**Local Kafka is not reachable from Databricks workspace.** The Docker Compose
stack here is for local development of the ingestion service. To feed Databricks
you have two practical options, documented in `DATABRICKS_SETUP.md`:

1. Point the producer at a managed Kafka (Confluent Cloud free tier, Redpanda
   Serverless) that Databricks can reach. Set `KAFKA_SECURITY_PROTOCOL=SASL_SSL`
   and the SASL credentials in `.env`.
2. Run the file-sink mode of the producer, which mirrors every Kafka message to
   newline-delimited JSON in a directory that you upload to a Unity Catalog
   volume. The bronze job has a `--source-type=volume` variant for this case.

The recommended path is (1) with Confluent Cloud's free tier. (2) is documented
for users who want a fully offline demo.

**Yelp Fusion ToS.** Yelp's terms prohibit long-term caching of business content
and require attribution. This project is intended for short-lived demonstration
and personal analytics. The Silver/Gold tables retain only fields needed for
trend analysis (id, name, coordinates rounded, rating, review count, category
slugs, timestamps), not full reviews or photos. Anyone running this in a real
environment should re-read the
[Yelp API ToU](https://docs.developer.yelp.com/docs/fusion-tou) and add a TTL
job that prunes rows older than 24h, or swap the source to an unrestricted feed
(OpenStreetMap Overpass, OpenAQ, GDELT, etc.).

**No exactly-once guarantee end-to-end.** The producer is idempotent within a
Kafka session, and the Spark sink uses checkpointed offsets, so the path is
effectively at-least-once with idempotent upserts in Silver (MERGE on
`business_id` + `observed_at`). Duplicates that survive the MERGE are rare but
possible if the producer is restarted mid-flight.

**Schema evolution.** The Bronze table stores the raw envelope as a `STRING`
column and decodes lazily in Silver. This lets us survive Yelp adding fields
without a Bronze migration. Field removals or type changes still require a
Silver job update.

---

## Engineering decisions

A few choices that would otherwise look odd:

- **confluent-kafka over kafka-python.** The librdkafka-backed client has better
  throughput, idempotent producer support, and a real `flush()` semantic. The
  pure-Python client is fine for toy work but drops messages under load.
- **pydantic v2 for envelopes, not Avro.** A schema registry would be the right
  call at scale, but it adds a service to operate. Pydantic gives us the
  validation surface and JSON serialization without the operational footprint.
  Migration to Avro + Schema Registry is noted in Future work.
- **No `__init__.py` magic.** Modules import explicitly. There is no plugin
  discovery, no decorators that mutate globals, no implicit config side effects
  on import. This makes the code easier to debug at 2am.
- **One config object, frozen at startup.** `config.settings.Settings` reads
  from environment once and is immutable for the process lifetime. Tests pass
  in their own instance instead of monkeypatching env vars.

---

## Future work

- Confluent Schema Registry + Avro for the raw topic.
- Replace polling with webhook ingestion if/when Yelp adds it; the producer
  contract already supports event-shaped input.
- Add a Great Expectations suite that runs against Silver on each batch.
- Wire Databricks Lakeflow Jobs for orchestration instead of the cron-style
  `availableNow` loop.
- A second source connector (OpenStreetMap Overpass for location-only events)
  to demonstrate multi-source fan-in.

---

## License

MIT. See `LICENSE`.
