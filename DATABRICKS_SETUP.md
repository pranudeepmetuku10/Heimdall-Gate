# Databricks Free Edition setup

This guide walks through wiring Heimdall-Gate's Spark layer on Databricks Free
Edition. Read the limitations section in the main `README.md` first; the
serverless model dictates the operating shape below.

The goal of this document is a reproducible demo. By the end you will have:

- A Unity Catalog with `bronze`, `silver`, `gold`, `dlq` schemas
- Two Databricks jobs scheduled with `trigger(availableNow=True)`
- A working Kafka -> Bronze -> Silver -> Gold path
- A quarantine table populated with deliberately bad records

Total setup time is around 30-45 minutes the first time.

---

## 0. What you need before starting

- A Databricks Free Edition account: https://www.databricks.com/learn/free-edition
- A Yelp Fusion API key: https://docs.developer.yelp.com/docs/fusion-intro
- A managed Kafka that Databricks can reach over the internet. Recommended:
  Confluent Cloud free tier (https://www.confluent.io/confluent-cloud/tryfree/).
  Alternatives: Redpanda Serverless, Aiven Kafka free tier.
- Python 3.11+ locally
- Docker (only if you want to also test against a local broker)

If you cannot or do not want to use a cloud Kafka, jump to
[Appendix A: Volume-source mode](#appendix-a-volume-source-mode-no-cloud-kafka).
The pipeline still runs, it just reads from a Unity Catalog volume populated by
the local producer instead of a live Kafka topic.

---

## 1. Provision a cloud Kafka (Confluent Cloud free tier)

1. Sign up at https://confluent.cloud/signup. The free tier gives $400 of usage
   credit and a Basic cluster, which is enough for this demo.
2. Create a Basic cluster in a region geographically near your Databricks
   workspace (typically `us-east-1` / `us-west-2` for the Databricks Free
   Edition serverless region).
3. In the cluster, go to **API keys** and create a new key with Global access.
   Note the key and secret.
4. Go to **Topics** and create:
   - `heimdall.raw.business` (6 partitions, retention 7 days)
   - `heimdall.dlq.ingest` (3 partitions, retention 7 days)
5. From **Cluster settings > Endpoints**, copy the bootstrap server URL (looks
   like `pkc-xxxxx.us-east-1.aws.confluent.cloud:9092`).

Fill these into your local `.env`:

```
KAFKA_BOOTSTRAP_SERVERS=pkc-xxxxx.us-east-1.aws.confluent.cloud:9092
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=PLAIN
KAFKA_SASL_USERNAME=<API key>
KAFKA_SASL_PASSWORD=<API secret>
KAFKA_TOPIC_RAW=heimdall.raw.business
KAFKA_TOPIC_DLQ=heimdall.dlq.ingest
```

Smoke test the producer locally before involving Databricks:

```bash
make install
make ingest-once
```

You should see records appear in Confluent Cloud's topic browser within a few
seconds. If they do not, fix this before continuing. Connectivity bugs from
Databricks are harder to diagnose than connectivity bugs from your laptop.

---

## 2. Bootstrap Unity Catalog objects

In the Databricks workspace, open the SQL editor and run:

```sql
CREATE CATALOG IF NOT EXISTS heimdall;
USE CATALOG heimdall;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS dlq;

CREATE VOLUME IF NOT EXISTS heimdall.bronze.checkpoints;
CREATE VOLUME IF NOT EXISTS heimdall.bronze.raw_drop;
```

The `raw_drop` volume is only used in volume-source mode (Appendix A). Creating
it now is harmless.

---

## 3. Upload the Spark jobs

There are two equivalent paths. Pick one.

### Option A: Repos (recommended)

1. In Databricks, go to **Workspace > Repos** and click **Add Repo**.
2. Point it at your fork of this repository on GitHub.
3. The job sources will be at `Repos/<you>/heimdall-gate/databricks/jobs/`.

This is the cleanest path because you can iterate on the jobs locally and pull
the latest version with a button.

### Option B: Workspace files

1. In **Workspace > Users > your.email > heimdall-gate**, create a folder.
2. Upload the four files from `databricks/jobs/` into it:
   - `00_bootstrap_tables.py`
   - `01_bronze_ingest.py`
   - `02_silver_transform.py`
   - `03_gold_aggregates.py`

---

## 4. Store Kafka credentials in a Databricks secret scope

Do not paste Kafka credentials into job parameters. Use a secret scope.

From a notebook or the Databricks CLI:

```bash
databricks secrets create-scope heimdall
databricks secrets put-secret heimdall kafka_bootstrap --string-value "pkc-xxxxx.us-east-1.aws.confluent.cloud:9092"
databricks secrets put-secret heimdall kafka_username  --string-value "<API key>"
databricks secrets put-secret heimdall kafka_password  --string-value "<API secret>"
```

The Spark jobs read these via `dbutils.secrets.get(scope="heimdall", key=...)`.

---

## 5. Run the bootstrap job

Open `00_bootstrap_tables.py` in the workspace and run it once as a notebook
(top-right **Run all**). It is idempotent and creates the Delta tables with the
expected schema.

You should see the following tables when you finish:

```
heimdall.bronze.business_events_raw
heimdall.silver.business_events
heimdall.gold.rating_drift_daily
heimdall.gold.category_trends_daily
heimdall.dlq.business
```

Verify with:

```sql
SHOW TABLES IN heimdall.bronze;
SHOW TABLES IN heimdall.silver;
SHOW TABLES IN heimdall.gold;
SHOW TABLES IN heimdall.dlq;
```

---

## 6. Create the Bronze job

In **Workflows > Jobs > Create job**:

- Name: `heimdall-bronze`
- Task name: `bronze_ingest`
- Type: `Python script` (or `Notebook` if you uploaded as `.py` notebooks)
- Source: workspace path to `01_bronze_ingest.py`
- Compute: **Serverless**
- Parameters (Job parameters tab):
  - `source_type=kafka`
  - `kafka_topic=heimdall.raw.business`
  - `bronze_table=heimdall.bronze.business_events_raw`
  - `checkpoint_location=/Volumes/heimdall/bronze/checkpoints/bronze_ingest`
- Schedule: every 10 minutes (cron `0 */10 * * * ?`). Adjust to taste.

Run it manually once to confirm it succeeds. The first run will create the
checkpoint directory and process whatever is currently in the Kafka topic. Each
subsequent run only picks up new offsets.

Verify rows landed:

```sql
SELECT COUNT(*), MAX(ingest_ts) FROM heimdall.bronze.business_events_raw;
```

---

## 7. Create the Silver job

Same flow as Bronze.

- Name: `heimdall-silver`
- Source: `02_silver_transform.py`
- Compute: **Serverless**
- Parameters:
  - `bronze_table=heimdall.bronze.business_events_raw`
  - `silver_table=heimdall.silver.business_events`
  - `dlq_table=heimdall.dlq.business`
  - `checkpoint_location=/Volumes/heimdall/bronze/checkpoints/silver_transform`
- Schedule: every 10 minutes, offset by a few minutes so Bronze finishes first.
  Or chain it: in the job UI, add Silver as a downstream task of Bronze in the
  same job. Chaining is preferred.

This job reads Bronze as a streaming source (Delta supports CDC reads),
validates with the shared rules in `validation/validators.py` (re-implemented
in PySpark inside the job), MERGEs valid rows into Silver, and writes failures
to the DLQ table.

---

## 8. Create the Gold job

- Name: `heimdall-gold`
- Source: `03_gold_aggregates.py`
- Compute: **Serverless**
- Parameters:
  - `silver_table=heimdall.silver.business_events`
  - `gold_rating_drift_table=heimdall.gold.rating_drift_daily`
  - `gold_category_trends_table=heimdall.gold.category_trends_daily`
- Schedule: chained after Silver, or hourly.

This job is batch (not streaming). It overwrites the daily partitions for the
current and previous day so late-arriving events are reflected. Earlier
partitions are immutable.

---

## 9. Start the ingestion service

On your laptop, with `.env` pointed at Confluent Cloud:

```bash
make ingest
```

Within a poll cycle you should see:

1. Records appearing in Confluent Cloud's `heimdall.raw.business` topic browser
2. Bronze row count climbing on the next Databricks scheduled run
3. Silver populated shortly after
4. DLQ table populated only if you intentionally introduce bad records (see
   `scripts/smoke_test.sh --inject-bad`)

---

## 10. Verify with the demo queries

In the SQL editor:

```sql
-- 10 most recent events in Silver
SELECT business_id, name, city, rating, review_count, observed_at
FROM heimdall.silver.business_events
ORDER BY observed_at DESC
LIMIT 10;

-- Distribution of validation failures
SELECT failure_reason, COUNT(*) AS n
FROM heimdall.dlq.business
GROUP BY failure_reason
ORDER BY n DESC;

-- Rating drift for a known business (replace business_id)
SELECT day, rating_avg, rating_min, rating_max, observations
FROM heimdall.gold.rating_drift_daily
WHERE business_id = 'WavvLdfdP6g8aZTtbBQHTw'
ORDER BY day DESC
LIMIT 14;

-- Top categories last 7 days
SELECT category, city, total_observations, distinct_businesses
FROM heimdall.gold.category_trends_daily
WHERE day >= current_date() - INTERVAL 7 DAYS
ORDER BY total_observations DESC
LIMIT 25;
```

---

## Troubleshooting

**Bronze job succeeds but no rows arrive.**
The producer is probably writing to a topic Databricks isn't reading. Open
Confluent Cloud's topic browser and confirm messages exist. Then double-check
the `kafka_topic` job parameter matches.

**Bronze job fails with `Failed to construct kafka consumer`.**
The SASL credentials in the secret scope are wrong, or the bootstrap server URL
has a typo. Run `databricks secrets list-secrets heimdall` to confirm the keys
exist, then re-put them.

**Silver MERGE fails with `cannot resolve column`.**
The Bronze envelope schema drifted (Yelp added a field). The Silver job pulls
fields by `get_json_object`, so this should not crash; if it does, the envelope
itself changed shape. Inspect the most recent Bronze row.

**`AnalysisException: Path does not exist` on first run.**
The checkpoint volume wasn't created. Re-run the SQL from step 2 and verify
with `LIST /Volumes/heimdall/bronze/checkpoints/`.

**Free Edition compute keeps timing out.**
Free Edition serverless has aggressive idle timeouts. This is expected. The
`availableNow` trigger means the job spins up, drains, and exits. If a job is
running for more than 5-10 minutes per batch on this volume of data, the issue
is probably a non-pruning predicate in the Silver MERGE.

---

## Appendix A: Volume-source mode (no cloud Kafka)

If you cannot use Confluent Cloud, you can run the demo entirely from local
files synced to a Unity Catalog volume. The tradeoff is that you lose the
"actually streaming through Kafka" property; you keep the Spark Structured
Streaming semantics by using `cloudFiles` (Auto Loader) on the volume.

1. Set `HEIMDALL_FILE_SINK_PATH=./out/raw_drop` in `.env`.
2. Set `HEIMDALL_FILE_SINK_ENABLED=true`.
3. Run `make ingest`. The producer now writes JSONL files in addition to
   publishing to Kafka. If Kafka is unreachable, set
   `KAFKA_BOOTSTRAP_SERVERS=` (empty) to skip the Kafka publish entirely.
4. Upload the contents of `./out/raw_drop` to
   `/Volumes/heimdall/bronze/raw_drop/` via:
   ```bash
   databricks fs cp ./out/raw_drop dbfs:/Volumes/heimdall/bronze/raw_drop --recursive --overwrite
   ```
5. In the Bronze job parameters, set:
   - `source_type=volume`
   - `volume_path=/Volumes/heimdall/bronze/raw_drop`

The Bronze job branches on `source_type` and reads with Auto Loader instead of
the Kafka source. Everything downstream is identical.

This is also the recommended path for unit-testing Spark logic locally with
`pyspark` before pushing to Databricks.

---

## Appendix B: Cleaning up

```sql
DROP CATALOG IF EXISTS heimdall CASCADE;
```

In the Workflows UI, delete the three jobs. In Confluent Cloud, delete the
cluster (or just the topics if you want to keep the free credit). On your
laptop, `make kafka-down` stops the local broker.
