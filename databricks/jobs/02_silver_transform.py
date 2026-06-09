# Databricks job: heimdall-silver
#
# Reads incrementally from bronze.business_events_raw, parses the envelope,
# applies validation, and writes:
#   - valid rows -> silver.business_events (MERGE on business_id+observed_at)
#   - invalid    -> dlq.business
#
# Trigger: availableNow. Idempotent on at-least-once delivery.
#
# Job parameters:
#   bronze_table         heimdall.bronze.business_events_raw
#   silver_table         heimdall.silver.business_events
#   dlq_table            heimdall.dlq.business
#   checkpoint_location  /Volumes/heimdall/bronze/checkpoints/silver_transform
#
# Validation rules mirror validation/validators.py (Python). When you change
# rules there, mirror the change here.

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


def _annotate_failures(parsed: DataFrame) -> DataFrame:
    """Add a `failure_codes ARRAY<STRING>` column listing every rule a row
    violated. Rows with `size(failure_codes) = 0` are valid."""

    p = F.col("env.payload")
    coords = p["coordinates"]
    loc = p["location"]

    checks = [
        (
            "missing_business_id",
            (p["business_id"].isNull()) | (F.length(F.trim(p["business_id"])) == 0),
        ),
        (
            "missing_name",
            (p["name"].isNull()) | (F.length(F.trim(p["name"])) == 0),
        ),
        ("missing_rating", p["rating"].isNull()),
        (
            "rating_out_of_range",
            p["rating"].isNotNull()
            & ~F.round(p["rating"], 1).isin(VALID_RATINGS),
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
        F.array(*[
            F.when(cond, F.lit(code)).otherwise(F.lit(None))
            for code, cond in checks
        ])
    )
    return parsed.withColumn("failure_codes", code_array)


def main() -> None:
    spark = SparkSession.builder.getOrCreate()

    bronze_table = _arg("bronze_table", "heimdall.bronze.business_events_raw")
    silver_table = _arg("silver_table", "heimdall.silver.business_events")
    dlq_table = _arg("dlq_table", "heimdall.dlq.business")
    checkpoint = _arg(
        "checkpoint_location",
        "/Volumes/heimdall/bronze/checkpoints/silver_transform",
    )

    stream = (
        spark.readStream
        .format("delta")
        .option("ignoreChanges", "false")
        .table(bronze_table)
    )

    parsed = (
        stream
        .withColumn("env", F.from_json(F.col("raw_envelope"), ENVELOPE_SCHEMA))
        .filter(F.col("env").isNotNull())
    )

    annotated = _annotate_failures(parsed)

    # MicroBatch handler that splits good/bad and writes both atomically(ish).
    def upsert(batch_df: DataFrame, batch_id: int) -> None:
        # Cache because we read it twice (silver + dlq).
        batch_df = batch_df.persist()
        try:
            good = (
                batch_df.filter(F.size(F.col("failure_codes")) == 0)
                .select(
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
                )
                # Deduplicate within the micro-batch: latest observed_at wins.
                .withColumn(
                    "_rn",
                    F.row_number().over(
                        Window.partitionBy("business_id", "observed_at")
                        .orderBy(F.col("ingest_ts").desc())
                    ),
                )
                .filter(F.col("_rn") == 1)
                .drop("_rn")
            )

            target = DeltaTable.forName(spark, silver_table)
            (
                target.alias("t")
                .merge(
                    good.alias("s"),
                    "t.business_id = s.business_id AND t.observed_at = s.observed_at",
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

            bad = (
                batch_df.filter(F.size(F.col("failure_codes")) > 0)
                .select(
                    F.col("ingest_ts"),
                    F.col("kafka_topic"),
                    F.col("kafka_offset"),
                    F.col("kafka_key"),
                    F.lit("silver_validation").alias("failure_reason"),
                    F.col("failure_codes"),
                    F.col("raw_envelope"),
                )
            )
            (
                bad.write
                .format("delta")
                .mode("append")
                .saveAsTable(dlq_table)
            )

            valid_n = good.count()
            bad_n = bad.count()
            print(
                f"silver.batch id={batch_id} valid={valid_n} dlq={bad_n}"
            )
        finally:
            batch_df.unpersist()

    query = (
        annotated.writeStream
        .foreachBatch(upsert)
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    print(
        f"silver.done table={silver_table} "
        f"input_rows={query.lastProgress.get('numInputRows') if query.lastProgress else 0}"
    )


if __name__ == "__main__":
    main()
