"""Gold body metrics: weight and VO2max over time.

Measurement dates are local: to_date() of a UTC timestamp uses the Spark session time
zone, which the gold job sets (see jobs/gold_polar.py).
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from wearable_sync_pipelines.gold.periods import DAY_WEEK_MONTH, aggregate_by_period


def weight_by_period(body_measurements: DataFrame) -> DataFrame:
    """Weight per day, week and month: mean with standard deviation, min, max and count.

    The standard deviation is NULL for a period with a single measurement.
    """
    weights = body_measurements.where(F.col("weight_kg").isNotNull()).select(
        F.to_date("measured_at").alias("date"), "weight_kg"
    )
    return aggregate_by_period(
        weights,
        date_column="date",
        granularities=DAY_WEEK_MONTH,
        aggregations=[
            F.count("*").alias("measurements"),
            F.avg("weight_kg").alias("weight_mean_kg"),
            F.stddev_samp("weight_kg").alias("weight_std_kg"),
            F.min("weight_kg").alias("weight_min_kg"),
            F.max("weight_kg").alias("weight_max_kg"),
        ],
    )


def vo2max_by_period(body_measurements: DataFrame) -> DataFrame:
    """VO2max per day, week and month: the latest value in the period.

    VO2max changes in steps (after a fitness test or an estimate update), so a mean or
    a spread would not describe it.
    """
    values = body_measurements.where(F.col("vo2max").isNotNull()).select(
        F.to_date("measured_at").alias("date"), "measured_at", "vo2max"
    )
    return aggregate_by_period(
        values,
        date_column="date",
        granularities=DAY_WEEK_MONTH,
        aggregations=[F.max_by("vo2max", "measured_at").alias("vo2max")],
    )
