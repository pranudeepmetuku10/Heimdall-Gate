# Databricks job: heimdall-gold
#
# Batch (not streaming). Recomputes gold partitions for the current and
# previous calendar day so late-arriving Silver rows are reflected. Older
# partitions are immutable - we do not touch them, which lets historical
# analytics queries hit a stable surface.
#
# Job parameters:
#   silver_table                 heimdall.silver.business_events
#   gold_rating_drift_table      heimdall.gold.rating_drift_daily
#   gold_category_trends_table   heimdall.gold.category_trends_daily

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def _arg(name: str, default: str | None = None) -> str:
    try:
        dbutils.widgets.text(name, default or "")  # noqa: F821
        v = dbutils.widgets.get(name)              # noqa: F821
    except Exception:
        v = default or ""
    if not v and default is None:
        raise ValueError(f"required job parameter '{name}' is empty")
    return v or default or ""


def main() -> None:
    spark = SparkSession.builder.getOrCreate()

    silver_table = _arg("silver_table", "heimdall.silver.business_events")
    rating_drift_table = _arg(
        "gold_rating_drift_table", "heimdall.gold.rating_drift_daily"
    )
    category_trends_table = _arg(
        "gold_category_trends_table", "heimdall.gold.category_trends_daily"
    )

    silver = spark.table(silver_table)

    # Restrict to current + previous day. Anything older is considered
    # finalized and is not recomputed.
    cutoff_lower = F.date_sub(F.current_date(), 1)
    bounded = silver.filter(F.to_date("observed_at") >= cutoff_lower)
    bounded = bounded.withColumn("day", F.to_date("observed_at"))

    # -- Rating drift -----------------------------------------------------
    rating_drift = (
        bounded
        .groupBy("day", "business_id", "name", "city")
        .agg(
            F.avg("rating").alias("rating_avg"),
            F.min("rating").alias("rating_min"),
            F.max("rating").alias("rating_max"),
            F.max("review_count").alias("review_count_max"),
            F.count(F.lit(1)).cast("int").alias("observations"),
        )
    )

    (
        rating_drift.write
        .format("delta")
        .mode("overwrite")
        # Replace only the partitions we recomputed.
        .option(
            "replaceWhere",
            f"day >= date_sub(current_date(), 1)",
        )
        .saveAsTable(rating_drift_table)
    )

    # -- Category trends --------------------------------------------------
    exploded = bounded.withColumn("category", F.explode_outer("categories"))
    category_trends = (
        exploded
        .filter(F.col("category").isNotNull())
        .groupBy("day", "category", "city")
        .agg(
            F.count(F.lit(1)).cast("long").alias("total_observations"),
            F.countDistinct("business_id").cast("long").alias("distinct_businesses"),
            F.avg("rating").alias("avg_rating"),
        )
    )
    (
        category_trends.write
        .format("delta")
        .mode("overwrite")
        .option(
            "replaceWhere",
            f"day >= date_sub(current_date(), 1)",
        )
        .saveAsTable(category_trends_table)
    )

    rd = rating_drift.count()
    ct = category_trends.count()
    print(f"gold.done rating_drift_rows={rd} category_trends_rows={ct}")


if __name__ == "__main__":
    main()
