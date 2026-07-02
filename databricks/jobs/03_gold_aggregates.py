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

import datetime as dt

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

    # Compute the recompute window ONCE, in the driver, as a literal date.
    # We reuse the same literal for both the read-side filter and the
    # Delta replaceWhere predicate. This matters: if we let current_date()
    # be evaluated independently on the read and the write, a run that
    # straddles midnight UTC would build rows for one day but assert a
    # replaceWhere bound for another, and Delta would reject the write with
    # "data written does not conform to replaceWhere". One literal removes
    # the race entirely.
    window_days = int(_arg("recompute_window_days", "2"))
    today = dt.datetime.now(dt.timezone.utc).date()
    as_of = today - dt.timedelta(days=window_days - 1)
    as_of_str = as_of.isoformat()
    replace_predicate = f"day >= DATE'{as_of_str}'"
    print(f"gold.window as_of={as_of_str} today={today} days={window_days}")

    silver = spark.table(silver_table)

    # Restrict to the recompute window. Anything older is considered finalized
    # and is not touched, so historical analytics hit a stable surface.
    bounded = (
        silver
        .withColumn("day", F.to_date("observed_at"))
        .filter(F.col("day") >= F.lit(as_of_str).cast("date"))
        # Reused by both aggregations below; cache to avoid re-scanning Silver.
        .cache()
    )

    try:
        # An empty window means "no new observations", not "delete the recent
        # partitions". replaceWhere + overwrite with zero rows would wipe every
        # partition matching the predicate, so short-circuit instead.
        if bounded.isEmpty():
            print(f"gold.skip empty window as_of={as_of_str}; partitions preserved")
            return

        # -- Rating drift, grain (day, business_id) -----------------------
        # name and city are mutable free-text attributes; keep the latest
        # observed value so the grain stays (day, business_id) as documented.
        rating_drift = (
            bounded
            .groupBy("day", "business_id")
            .agg(
                F.expr("max_by(name, observed_at)").alias("name"),
                F.expr("max_by(city, observed_at)").alias("city"),
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
            .option("replaceWhere", replace_predicate)
            .saveAsTable(rating_drift_table)
        )

        # -- Category trends, grain (day, category, city) -----------------
        category_trends = (
            bounded
            .withColumn("category", F.explode_outer("categories"))
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
            .option("replaceWhere", replace_predicate)
            .saveAsTable(category_trends_table)
        )

        print(f"gold.done rating_drift_rows={rating_drift.count()} "
              f"category_trends_rows={category_trends.count()}")
    finally:
        bounded.unpersist()


if __name__ == "__main__":
    main()
