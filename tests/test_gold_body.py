from datetime import UTC, datetime

import pytest
from pyspark.sql import DataFrame, SparkSession

from silver_frames import by_period, silver_frame
from wearable_sync_pipelines.gold import body
from wearable_sync_pipelines.polar import tables


def measurements(spark: SparkSession, *rows: tuple[str, float | None, int | None]) -> DataFrame:
    """Fictional body measurements: (UTC timestamp, weight_kg, vo2max)."""
    return silver_frame(
        spark,
        tables.BODY_MEASUREMENTS,
        [
            {
                "measurement_id": f"M{index}",
                "measured_at": datetime.fromisoformat(measured_at).replace(tzinfo=UTC),
                "weight_kg": weight,
                "vo2max": vo2max,
            }
            for index, (measured_at, weight, vo2max) in enumerate(rows)
        ],
    )


def test_weight_is_aggregated_per_day_week_and_month(spark: SparkSession) -> None:
    frame = measurements(
        spark,
        ("2026-11-02T06:00:00", 80.0, None),  # Monday
        ("2026-11-04T06:00:00", 81.0, None),
        ("2026-11-08T06:00:00", 82.0, None),  # Sunday, same week
        ("2026-11-09T06:00:00", 79.0, None),  # Monday, next week
    )

    result = by_period(
        body.weight_by_period(frame),
        "measurements",
        "weight_mean_kg",
        "weight_std_kg",
        "weight_min_kg",
        "weight_max_kg",
    )

    assert result[("day", "2026-11-02")] == (1, 80.0, None, 80.0, 80.0)
    assert result[("week", "2026-11-02")] == (3, 81.0, pytest.approx(1.0), 80.0, 82.0)
    assert result[("week", "2026-11-09")] == (1, 79.0, None, 79.0, 79.0)
    assert result[("month", "2026-11-01")][:2] == (4, 80.5)
    assert len(result) == 4 + 2 + 1


def test_weight_uses_the_local_date(spark: SparkSession) -> None:
    # 23:30 UTC on 31 October is 00:30 on 1 November in Warsaw (UTC+1).
    frame = measurements(spark, ("2026-10-31T23:30:00", 80.0, None))

    result = by_period(body.weight_by_period(frame), "measurements")

    assert set(result) == {("day", "2026-11-01"), ("week", "2026-10-26"), ("month", "2026-11-01")}


def test_snapshots_without_weight_are_ignored(spark: SparkSession) -> None:
    frame = measurements(
        spark, ("2026-11-02T06:00:00", 80.0, None), ("2026-11-02T18:00:00", None, 50)
    )

    result = by_period(body.weight_by_period(frame), "measurements", "weight_mean_kg")

    assert result[("day", "2026-11-02")] == (1, 80.0)


def test_vo2max_is_the_latest_value_in_the_period(spark: SparkSession) -> None:
    frame = measurements(
        spark,
        ("2026-11-08T06:00:00", 80.0, 50),
        ("2026-11-02T06:00:00", 80.0, 48),
        ("2026-11-04T06:00:00", 80.0, None),
    )

    result = by_period(body.vo2max_by_period(frame), "vo2max")

    assert result[("week", "2026-11-02")] == (50,)
    assert result[("month", "2026-11-01")] == (50,)
    assert result[("day", "2026-11-02")] == (48,)
    assert ("day", "2026-11-04") not in result
