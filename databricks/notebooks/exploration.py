# Databricks notebook source
# Exploratory queries against Heimdall tables. Not part of the scheduled
# pipeline; safe to run interactively.

# COMMAND ----------

# DBTITLE 1, Row counts across the medallion
display(spark.sql("""
    SELECT 'bronze.business_events_raw'   AS table_name, COUNT(*) AS rows FROM heimdall.bronze.business_events_raw
    UNION ALL SELECT 'silver.business_events',          COUNT(*) FROM heimdall.silver.business_events
    UNION ALL SELECT 'gold.rating_drift_daily',         COUNT(*) FROM heimdall.gold.rating_drift_daily
    UNION ALL SELECT 'gold.category_trends_daily',      COUNT(*) FROM heimdall.gold.category_trends_daily
    UNION ALL SELECT 'dlq.business',                    COUNT(*) FROM heimdall.dlq.business
"""))

# COMMAND ----------

# DBTITLE 1, DLQ failure distribution
display(spark.sql("""
    SELECT failure_reason,
           failure_code,
           COUNT(*) AS n
    FROM (
        SELECT failure_reason, explode(failure_codes) AS failure_code
        FROM heimdall.dlq.business
    )
    GROUP BY failure_reason, failure_code
    ORDER BY n DESC
"""))

# COMMAND ----------

# DBTITLE 1, Rating drift sample
display(spark.sql("""
    SELECT day, business_id, name, city, rating_avg, rating_min, rating_max, observations
    FROM heimdall.gold.rating_drift_daily
    WHERE day >= current_date() - INTERVAL 7 DAYS
    ORDER BY observations DESC
    LIMIT 50
"""))

# COMMAND ----------

# DBTITLE 1, Categories trending in the last week
display(spark.sql("""
    SELECT category, city, SUM(total_observations) AS observations,
           SUM(distinct_businesses) AS distinct_businesses_sum
    FROM heimdall.gold.category_trends_daily
    WHERE day >= current_date() - INTERVAL 7 DAYS
    GROUP BY category, city
    ORDER BY observations DESC
    LIMIT 25
"""))
