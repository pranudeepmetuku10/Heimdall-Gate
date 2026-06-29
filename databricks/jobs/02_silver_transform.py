# Databricks job: heimdall-silver
#
# Reads incrementally from bronze.business_events_raw, parses the envelope,
# validates it, and writes:
#   - valid rows   -> silver.business_events  (MERGE on event_id)
#   - invalid rows -> dlq.business            (MERGE on dlq_key)
#
# Trigger: availableNow.
#
# Idempotency model. Silver is a time series of validated business
# observations, one row per source event. The MERGE key is the envelope's
# event_id, which is minted once by the producer and frozen in Bronze. So
# reprocessing the same Bronze offsets (e.g. after clearing this job's
# checkpoint) updates rows in place instead of inserting duplicates. A later
# re-fetch of the same business is a NEW observation with a new event_id and a
# new observed_at; that is intended and is what powers rating-drift analysis.
# The MERGE is a re-load guard, not a collapse of repeated observations.
#
# Nothing is dropped. Rows whose envelope fails to parse, or that fail any
# business rule, are routed to dlq.business with a reason and the original
# payload. The DLQ write is itself idempotent (MERGE on a content hash) so an
# executor retry of a micro-batch cannot double-count the data-quality metric.
#
# Job parameters:
#   bronze_table         heimdall.bronze.business_events_raw
#   silver_table         heimdall.silver.business_events
#   dlq_table            heimdall.dlq.business
#   checkpoint_location  /Volumes/heimdall/bronze/checkpoints/silver_transform
#
# Validation rules mirror validation/validators.py (Python). When you change a
# rule there, mirror it in _annotate_failures below.

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
from delta.tables import DeltaTable


def _arg(name: str, default: str | None = None) -> str:
    try:
        dbutils.widgets.text(name, default or "")  # noqa: F821
        v = dbutils.widgets.get(name)              # noqa: F821
    except Exception:
        v = default or ""
    if not v and default is None:
        raise ValueError(f"required job parameter '{name}' is empty")
    return v or default or ""


# Envelope schema. Kept in sync with ingest/schemas.py:Envelope.
ENVELOPE_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("producer_id", T.StringType()),
    T.StructField("source", T.StringType()),
    T.StructField("source_endpoint", T.StringType()),
    T.StructField("schema_version", T.IntegerType()),
    T.StructField("observed_at", T.StringType()),
    T.StructField("ingest_lag_ms", T.IntegerType()),
    T.StructField("payload", T.StructType([
        T.StructField("business_id", T.StringType()),
        T.StructField("name", T.StringType()),
        T.StructField("url", T.StringType()),
        T.StructField("image_url", T.StringType()),
        T.StructField("is_closed", T.BooleanType()),
        T.StructField("rating", T.DoubleType()),
        T.StructField("review_count", T.IntegerType()),
        T.StructField("price", T.StringType()),
        T.StructField("phone", T.StringType()),
        T.StructField("categories", T.ArrayType(T.StringType())),
        T.StructField("coordinates", T.StructType([
            T.StructField("latitude", T.DoubleType()),
            T.StructField("longitude", T.DoubleType()),
        ])),
        T.StructField("location", T.StructType([
            T.StructField("address1", T.StringType()),
            T.StructField("city", T.StringType()),
            T.StructField("state", T.StringType()),
            T.StructField("country", T.StringType()),
            T.StructField("zip_code", T.StringType()),
        ])),
        T.StructField("transactions", T.ArrayType(T.StringType())),
        T.StructField("search_term", T.StringType()),
        T.StructField("search_location", T.StringType()),
    ])),
])

VALID_RATINGS = [round(x * 0.5, 1) for x in range(2, 11)]  # 1.0..5.0

# Columns projected into Silver, in table order.
SILVER_COLUMNS = [
    "business_id", "name", "rating", "review_count", "price", "is_closed",
    "url", "categories", "latitude", "longitude", "city", "state", "country",
    "zip_code", "search_term", "search_location", "observed_at", "ingest_ts",
    "event_id", "producer_id", "schema_version",
]

# Columns in the DLQ, in table order. dlq_key is a stable content hash used as
# the MERGE key so a retried batch does not append the same bad row twice.
DLQ_COLUMNS = [
    "dlq_key", "ingest_ts", "kafka_topic", "kafka_partition", "kafka_offset",
    "kafka_key", "failure_reason", "failure_codes", "raw_envelope",
]


def _annotate_failures(parsed_ok: DataFrame) -> DataFrame:
    """Add `failure_codes ARRAY<STRING>` listing every rule a row violated.

    Operates on rows whose envelope parsed (env is not null). Rows with
    size(failure_codes) = 0 are valid.
    """
    p = F.col("env.payload")
    coords = p["coordinates"]
    loc = p["location"]
    event_id = F.col("env.event_id")
    observed_at = F.to_timestamp(F.col("env.observed_at"))

    checks = [
        # The MERGE keys and the Gold time bucket must never be null.
        (
            "missing_event_id",
            event_id.isNull() | (F.length(F.trim(event_id)) == 0),
        ),
        ("missing_observed_at", observed_at.isNull()),
        (
            "missing_business_id",
            p["business_id"].isNull() | (F.length(F.trim(p["business_id"])) == 0),
        ),
        (
            "missing_name",
            p["name"].isNull() | (F.length(F.trim(p["name"])) == 0),
        ),
        ("missing_rating", p["rating"].isNull()),
        (
            "rating_out_of_range",
            p["rating"].isNotNull() & ~F.round(p["rating"], 1).isin(VALID_RATINGS),
        ),
        ("missing_review_count", p["review_count"].isNull()),
        (
            "negative_review_count",
            p["review_count"].isNotNull() & (p["review_count"] < 0),
        ),
        (
            "missing_coordinates",
            coords["latitude"].isNull() | coords["longitude"].isNull(),
        ),
        (
            "latitude_out_of_range",
            coords["latitude"].isNotNull()
            & ((coords["latitude"] < -90) | (coords["latitude"] > 90)),
        ),
        (
            "longitude_out_of_range",
            coords["longitude"].isNotNull()
            & ((coords["longitude"] < -180) | (coords["longitude"] > 180)),
        ),
        (
            "missing_city",
            loc["city"].isNull() | (F.length(F.trim(loc["city"])) == 0),
        ),
        (
            "missing_categories",
            p["categories"].isNull() | (F.size(p["categories"]) == 0),
        ),
    ]

    code_array = F.array_compact(
        F.array(*[F.when(cond, F.lit(code)) for code, cond in checks])
    )
    return parsed_ok.withColumn("failure_codes", code_array)


def _to_silver(valid: DataFrame) -> DataFrame:
    """Project a validated, env-parsed frame onto the Silver schema and keep
    one row per event_id within the batch (deterministic tie-break)."""
    projected = valid.select(
        F.col("env.payload.business_id").alias("business_id"),
        F.col("env.payload.name").alias("name"),
        F.col("env.payload.rating").alias("rating"),
        F.col("env.payload.review_count").alias("review_count"),
        F.col("env.payload.price").alias("price"),
        F.col("env.payload.is_closed").alias("is_closed"),
        F.col("env.payload.url").alias("url"),
        F.col("env.payload.categories").alias("categories"),
        F.col("env.payload.coordinates.latitude").alias("latitude"),
        F.col("env.payload.coordinates.longitude").alias("longitude"),
        F.col("env.payload.location.city").alias("city"),
        F.col("env.payload.location.state").alias("state"),
        F.col("env.payload.location.country").alias("country"),
        F.col("env.payload.location.zip_code").alias("zip_code"),
        F.col("env.payload.search_term").alias("search_term"),
        F.col("env.payload.search_location").alias("search_location"),
        F.to_timestamp(F.col("env.observed_at")).alias("observed_at"),
        F.col("ingest_ts"),
        F.col("env.event_id").alias("event_id"),
        F.col("env.producer_id").alias("producer_id"),
        F.col("env.schema_version").alias("schema_version"),
        F.col("kafka_offset"),
    )
    # The same event_id can appear twice in a batch only if Bronze ingested the
    # same envelope twice; those rows are byte-identical, so any one will do.
    # Order deterministically anyway so the chosen row never depends on shuffle.
    dedup = (
        projected
        .withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy("event_id")
                .orderBy(F.col("ingest_ts").desc(), F.col("kafka_offset").desc())
            ),
        )
        .filter(F.col("_rn") == 1)
    )
    return dedup.select(*SILVER_COLUMNS)


def _to_dlq(rows: DataFrame, reason: str) -> DataFrame:
    """Project quarantined rows onto the DLQ schema with a stable content key."""
    return rows.select(
        F.sha2(
            F.concat_ws(
                "§",
                F.col("kafka_topic"),
                F.col("kafka_partition").cast("string"),
                F.col("kafka_offset").cast("string"),
                F.col("raw_envelope"),
            ),
            256,
        ).alias("dlq_key"),
        F.col("ingest_ts"),
        F.col("kafka_topic"),
        F.col("kafka_partition"),
        F.col("kafka_offset"),
        F.col("kafka_key"),
        F.lit(reason).alias("failure_reason"),
        F.col("failure_codes"),
        F.col("raw_envelope"),
    )


def main() -> None:
    spark = SparkSession.builder.getOrCreate()

    bronze_table = _arg("bronze_table", "heimdall.bronze.business_events_raw")
    silver_table = _arg("silver_table", "heimdall.silver.business_events")
    dlq_table = _arg("dlq_table", "heimdall.dlq.business")
    checkpoint = _arg(
        "checkpoint_location",
        "/Volumes/heimdall/bronze/checkpoints/silver_transform",
    )

    # Bronze is append-only, so the default Delta streaming read is correct.
    stream = (
        spark.readStream
        .format("delta")
        .table(bronze_table)
        .withColumn("env", F.from_json(F.col("raw_envelope"), ENVELOPE_SCHEMA))
    )

    def upsert(batch_df: DataFrame, batch_id: int) -> None:
        batch_df = batch_df.persist()
        try:
            # A non-JSON or truncated Bronze value parses to a null struct. We
            # quarantine it rather than dropping it.
            parse_failed = batch_df.filter(F.col("env").isNull())
            parsed_ok = _annotate_failures(batch_df.filter(F.col("env").isNotNull()))

            good = _to_silver(parsed_ok.filter(F.size("failure_codes") == 0)).persist()
            try:
                silver = DeltaTable.forName(spark, silver_table)
                (
                    silver.alias("t")
                    .merge(good.alias("s"), "t.event_id = s.event_id")
                    .whenMatchedUpdateAll()
                    .whenNotMatchedInsertAll()
                    .execute()
                )

                dlq_rows = _to_dlq(
                    parsed_ok.filter(F.size("failure_codes") > 0), "silver_validation"
                ).unionByName(
                    _to_dlq(
                        parse_failed.withColumn(
                            "failure_codes", F.array(F.lit("unparseable_envelope"))
                        ),
                        "envelope_parse_failed",
                    )
                )
                dlq = DeltaTable.forName(spark, dlq_table)
                (
                    dlq.alias("t")
                    .merge(dlq_rows.alias("s"), "t.dlq_key = s.dlq_key")
                    .whenNotMatchedInsertAll()
                    .execute()
                )

                print(f"silver.batch id={batch_id} valid={good.count()} "
                      f"dlq={dlq_rows.count()}")
            finally:
                good.unpersist()
        finally:
            batch_df.unpersist()

    query = (
        stream.writeStream
        .foreachBatch(upsert)
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    last = query.lastProgress
    print(f"silver.done table={silver_table} "
          f"input_rows={last.get('numInputRows') if last else 0}")


if __name__ == "__main__":
    main()
