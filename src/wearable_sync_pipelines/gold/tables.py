"""Gold tables: where each one lives and how it is computed from silver."""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

from pyspark.sql import DataFrame

from wearable_sync_pipelines.gold import body, training

# Gold schemas, by subject; the job maps each to its (target-specific) schema name.
GoldSchema = Literal["body", "training"]


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


def _runs(silver: Silver) -> DataFrame:
    return training.runs(silver.exercises, silver.exercise_hr_zones, silver.exercise_samples)


RUNS = GoldTable(
    schema="training",
    name="runs",
    comment=(
        "One row per run (sport RUNNING): distance, duration, pace (min/km), heart rate, "
        "cadence (stops excluded), Polar running index and loads, and time and share of "
        "time in heart rate zones 1-5. run_date is the local date."
    ),
    build=_runs,
)

RUN_PROFILE = GoldTable(
    schema="training",
    name="run_profile",
    comment=(
        f"Each run in {training.PROFILE_SEGMENT_M} m segments: time, distance actually "
        "covered, pace, heart rate and cadence. For heart rate and pace profiles over "
        "distance and for comparing runs."
    ),
    build=lambda silver: training.run_segments(
        silver.exercises, silver.exercise_samples, training.PROFILE_SEGMENT_M
    ),
)

RUN_SPLITS = GoldTable(
    schema="training",
    name="run_splits",
    comment=(
        "Kilometre splits of each run: time, distance actually covered (shorter for the "
        "last, partial split), pace, heart rate and cadence."
    ),
    build=lambda silver: training.run_segments(
        silver.exercises, silver.exercise_samples, training.SPLIT_SEGMENT_M
    ),
)

RUNNING_BY_PERIOD = GoldTable(
    schema="training",
    name="running_by_period",
    comment=(
        "Running per week and month (granularity column): number of runs, distance, "
        "time, pace, duration-weighted heart rate, running index, loads and time in heart "
        "rate zones. Local dates, weeks start on Monday."
    ),
    build=lambda silver: training.running_by_period(_runs(silver)),
)

OTHER_SESSIONS_BY_PERIOD = GoldTable(
    schema="training",
    name="other_sessions_by_period",
    comment=(
        "Non-running sessions per week and month (granularity column) by activity "
        "(Polar detailed sport, e.g. STRENGTH_TRAINING): number of sessions and total "
        "duration. Local dates, weeks start on Monday."
    ),
    build=lambda silver: training.other_sessions_by_period(silver.exercises),
)

ALL_TABLES = (
    WEIGHT_BY_PERIOD,
    VO2MAX_BY_PERIOD,
    RUNS,
    RUN_PROFILE,
    RUN_SPLITS,
    RUNNING_BY_PERIOD,
    OTHER_SESSIONS_BY_PERIOD,
)
