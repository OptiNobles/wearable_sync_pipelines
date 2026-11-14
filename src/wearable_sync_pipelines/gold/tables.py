"""Gold tables: where each one lives and how it is computed from silver."""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

from pyspark.sql import DataFrame

from wearable_sync_pipelines.gold import body

# Gold schemas, by subject; the job maps each to its (target-specific) schema name.
GoldSchema = Literal["body"]


@dataclass(frozen=True)
class Silver:
    """The silver Polar tables gold is computed from."""

    exercises: DataFrame
    exercise_hr_zones: DataFrame
    exercise_samples: DataFrame
    body_measurements: DataFrame


@dataclass(frozen=True)
class GoldTable:
    schema: GoldSchema
    name: str
    comment: str
    build: Callable[[Silver], DataFrame]


WEIGHT_BY_PERIOD = GoldTable(
    schema="body",
    name="weight_by_period",
    comment=(
        "Weight per day, week and month (granularity column): mean, standard deviation, "
        "min, max and number of measurements. Local dates, weeks start on Monday."
    ),
    build=lambda silver: body.weight_by_period(silver.body_measurements),
)

VO2MAX_BY_PERIOD = GoldTable(
    schema="body",
    name="vo2max_by_period",
    comment=(
        "VO2max per day, week and month (granularity column): the latest value in the "
        "period. Local dates, weeks start on Monday."
    ),
    build=lambda silver: body.vo2max_by_period(silver.body_measurements),
)

ALL_TABLES = (WEIGHT_BY_PERIOD, VO2MAX_BY_PERIOD)
