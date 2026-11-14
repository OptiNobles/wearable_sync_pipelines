"""Calendar periods for gold aggregates.

Periods are local calendar dates; weeks start on Monday (ISO 8601). Every granularity
of a metric goes into the same table, with a 'granularity' column, so a dashboard
switches between them with a single filter.
"""

from collections.abc import Sequence
from functools import reduce
from typing import Literal

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

Granularity = Literal["day", "week", "month"]

DAY_WEEK_MONTH: tuple[Granularity, ...] = ("day", "week", "month")
WEEK_MONTH: tuple[Granularity, ...] = ("week", "month")


def period_start(date: Column, granularity: Granularity) -> Column:
    """First day of the period that contains 'date' (a DATE column)."""
    if granularity == "day":
        return date
    # trunc() to 'week' returns the Monday of the week.
    return F.trunc(date, granularity)


def aggregate_by_period(
    frame: DataFrame,
    *,
    date_column: str,
    granularities: Sequence[Granularity],
    aggregations: Sequence[Column],
    group_by: Sequence[str] = (),
) -> DataFrame:
    """One row per granularity, period start (and group), for every requested granularity."""
    per_granularity = [
        frame.groupBy(
            F.lit(granularity).alias("granularity"),
            period_start(F.col(date_column), granularity).alias("period_start"),
            *group_by,
        ).agg(*aggregations)
        for granularity in granularities
    ]
    return reduce(DataFrame.unionByName, per_granularity)
