# Databricks job: heimdall-bronze
#
# Reads from Kafka (or, in volume-source mode, from JSONL files in a Unity
# Catalog volume) and appends the raw envelope to bronze.business_events_raw.
#
# Trigger: availableNow. On each run we drain whatever is currently in the
# source and exit. The Databricks job scheduler should re-fire this every
# 5-30 minutes. This pattern fits Free Edition's serverless model better than
# a long-lived streaming application.
#
# Job parameters (set via Workflows > Job > Parameters):
#   source_type             "kafka" (default) or "volume"
#   kafka_topic             topic name (kafka mode)
#   volume_path             /Volumes/...      (volume mode)
#   bronze_table            heimdall.bronze.business_events_raw
#   checkpoint_location     /Volumes/heimdall/bronze/checkpoints/bronze_ingest
#
# Secrets:
#   heimdall/kafka_bootstrap
#   heimdall/kafka_username
#   heimdall/kafka_password

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def _arg(name: str, default: str | None = None) -> str:
    try:
        # Databricks widgets are the supported way to pass job parameters.
        dbutils.widgets.text(name, default or "")  # noqa: F821
        v = dbutils.widgets.get(name)              # noqa: F821
    except Exception:
        v = default or ""
    if not v and default is None:
        raise ValueError(f"required job parameter '{name}' is empty")
    return v or default or ""


def _secret(scope: str, key: str) -> str:
    try:
        return dbutils.secrets.get(scope=scope, key=key)  # noqa: F821
    except Exception:
        return ""


def main() -> None:
    spark = SparkSession.builder.getOrCreate()

    # PLAIN SASL requires the cleartext password inside the JAAS string, so it
    # is passed as a readStream option below. Mask it (and any other secret) in
    # query plans, the Spark UI, and StreamingQueryProgress JSON.
    spark.conf.set(
        "spark.sql.redaction.options.regex", "(?i)secret|password|token|jaas"
    )

    source_type = _arg("source_type", "kafka").lower()
    bronze_table = _arg("bronze_table", "heimdall.bronze.business_events_raw")
    checkpoint = _arg(
        "checkpoint_location",
        "/Volumes/heimdall/bronze/checkpoints/bronze_ingest",
    )

    if source_type == "kafka":
        topic = _arg("kafka_topic", "heimdall.raw.business")
        bootstrap = _secret("heimdall", "kafka_bootstrap")
        username = _secret("heimdall", "kafka_username")
        password = _secret("heimdall", "kafka_password")
        if not bootstrap:
            raise RuntimeError(
                "heimdall/kafka_bootstrap secret is empty; "
                "see DATABRICKS_SETUP.md step 4"
            )

        jaas = (
            "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule "
            f'required username="{username}" password="{password}";'
        )

        stream = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", bootstrap)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            # Fail loudly if the checkpoint expects offsets that Kafka
            # retention has already dropped, rather than silently skipping
            # them. Keep Kafka retention comfortably longer than the job's
            # schedule interval (see DATABRICKS_SETUP.md).
            .option("failOnDataLoss", "true")
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.mechanism", "PLAIN")
            .option("kafka.sasl.jaas.config", jaas)
            .load()
        )

        prepared = (
            stream
            .select(
                F.current_timestamp().alias("ingest_ts"),
                F.col("topic").alias("kafka_topic"),
                F.col("partition").alias("kafka_partition"),
                F.col("offset").alias("kafka_offset"),
                F.col("key").cast("string").alias("kafka_key"),
                F.col("value").cast("string").alias("raw_envelope"),
            )
        )

    elif source_type == "volume":
        volume_path = _arg("volume_path", "/Volumes/heimdall/bronze/raw_drop")

        stream = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "false")
            .option("cloudFiles.schemaLocation", f"{checkpoint}/_schema")
            .option("cloudFiles.includeExistingFiles", "true")
            .load(volume_path)
        )

        prepared = (
            stream
            .select(
                F.current_timestamp().alias("ingest_ts"),
                F.lit("file://volume").alias("kafka_topic"),
                F.lit(0).cast("int").alias("kafka_partition"),
                F.lit(-1).cast("long").alias("kafka_offset"),
                F.coalesce(F.col("payload.business_id"), F.lit("")).alias("kafka_key"),
                F.to_json(F.struct("*")).alias("raw_envelope"),
            )
        )

    else:
        raise ValueError(
            f"unknown source_type={source_type!r}; expected 'kafka' or 'volume'"
        )

    query = (
        prepared.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint)
        .option("mergeSchema", "false")
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(bronze_table)
    )

    # availableNow returns immediately; block until the trigger drains.
    query.awaitTermination()

    print(
        f"bronze.done source_type={source_type} table={bronze_table} "
        f"input_rows={query.lastProgress.get('numInputRows') if query.lastProgress else 0}"
    )


if __name__ == "__main__":
    main()
