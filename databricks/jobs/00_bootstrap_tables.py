# Databricks job: heimdall-bootstrap
#
# Idempotent setup of all Delta tables. Run once before the streaming jobs;
# safe to re-run after schema changes (it uses CREATE TABLE IF NOT EXISTS, so
# adding new columns to existing tables requires an ALTER, not a re-run).
#
# Free Edition notes:
#   - Compute: Serverless (any size).
#   - Catalog: assumes a Unity Catalog named `heimdall` already exists (see
#     DATABRICKS_SETUP.md step 2).

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

CATALOG = "heimdall"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.dlq")

# -- Bronze ---------------------------------------------------------------
# Append-only landing table. `raw_envelope` is the JSON-encoded envelope
# exactly as it landed on Kafka. We do not parse here so schema drift in the
# producer cannot break ingestion.
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.bronze.business_events_raw (
        ingest_ts        TIMESTAMP,
        kafka_topic      STRING,
        kafka_partition  INT,
        kafka_offset     BIGINT,
        kafka_key        STRING,
        raw_envelope     STRING
    )
    USING DELTA
    PARTITIONED BY (kafka_topic)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
    """
)

# -- Silver ---------------------------------------------------------------
# Validated, typed, MERGEd by (business_id, observed_at). Idempotent under
# at-least-once delivery.
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.silver.business_events (
        business_id       STRING NOT NULL,
        name              STRING,
        rating            DOUBLE,
        review_count      INT,
        price             STRING,
        is_closed         BOOLEAN,
        url               STRING,
        categories        ARRAY<STRING>,
        latitude          DOUBLE,
        longitude         DOUBLE,
        city              STRING,
        state             STRING,
        country           STRING,
        zip_code          STRING,
        search_term       STRING,
        search_location   STRING,
        observed_at       TIMESTAMP,
        ingest_ts         TIMESTAMP,
        event_id          STRING,
        producer_id       STRING,
        schema_version    INT
    )
    USING DELTA
    PARTITIONED BY (city)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
    """
)

# -- DLQ ------------------------------------------------------------------
# Where Silver sends records that fail validation. Stores enough to debug
# upstream without re-fetching from the API.
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.dlq.business (
        ingest_ts        TIMESTAMP,
        kafka_topic      STRING,
        kafka_offset     BIGINT,
        kafka_key        STRING,
        failure_reason   STRING,
        failure_codes    ARRAY<STRING>,
        raw_envelope     STRING
    )
    USING DELTA
    PARTITIONED BY (failure_reason)
    """
)

# -- Gold: rating drift ---------------------------------------------------
# One row per (business_id, day). Captures daily rating min/avg/max and
# observation count. Daily partitions are overwritten by the gold job for the
# current and previous calendar day so late-arriving events are reflected.
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.gold.rating_drift_daily (
        day              DATE,
        business_id      STRING,
        name             STRING,
        city             STRING,
        rating_avg       DOUBLE,
        rating_min       DOUBLE,
        rating_max       DOUBLE,
        review_count_max INT,
        observations     INT
    )
    USING DELTA
    PARTITIONED BY (day)
    """
)

# -- Gold: category trends ------------------------------------------------
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.gold.category_trends_daily (
        day                  DATE,
        category             STRING,
        city                 STRING,
        total_observations   BIGINT,
        distinct_businesses  BIGINT,
        avg_rating           DOUBLE
    )
    USING DELTA
    PARTITIONED BY (day)
    """
)

print("bootstrap.done catalog=" + CATALOG)
