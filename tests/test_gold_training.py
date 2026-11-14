from datetime import datetime
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession

from silver_frames import by_period, silver_frame
from wearable_sync_pipelines.gold import training
from wearable_sync_pipelines.polar import tables

# Fictional exercises (never real data): two runs and two other sessions in one week.
EXERCISES = [
    {
        "exercise_id": "RUN_A",
        "sport": "RUNNING",
        "detailed_sport": "RUNNING",
        "start_time_local": datetime(2026, 11, 2, 7, 0),  # Monday
        "duration_s": 3000.0,
        "distance_m": 10000.0,
        "hr_avg_bpm": 140,
        "running_index": 50,
        "cardio_load": 80.0,
        "perceived_load": 300.0,
    },
    {
        "exercise_id": "RUN_B",
        "sport": "RUNNING",
        "detailed_sport": "RUNNING",
        "start_time_local": datetime(2026, 11, 4, 18, 0),
        "duration_s": 1800.0,
        "distance_m": 5000.0,
        "hr_avg_bpm": 150,
        "running_index": 48,
        "cardio_load": 40.0,
        "perceived_load": 200.0,
    },
    {
        "exercise_id": "GYM",
        "sport": "OTHER",
        "detailed_sport": "STRENGTH_TRAINING",
        "start_time_local": datetime(2026, 11, 3, 17, 0),
        "duration_s": 3600.0,
    },
    {
        "exercise_id": "BIKE",
        "sport": "CYCLING",
        "start_time_local": datetime(2026, 11, 8, 10, 0),  # Sunday, same week
        "duration_s": 1200.0,
    },
]

HR_ZONES = [
    {"exercise_id": "RUN_A", "zone": 1, "time_in_zone_s": 600.0},
    {"exercise_id": "RUN_A", "zone": 2, "time_in_zone_s": 1200.0},
    {"exercise_id": "RUN_A", "zone": 3, "time_in_zone_s": 1200.0},
]


def run_a_samples() -> list[dict[str, Any]]:
    """RUN_A at a steady 4 m/s for 1200 m: 25 s per 100 m, 250 s per km.

    Heart rate 140 for the first 125 s, then 160; cadence 0 (standing) for 10 s.
    """
    return [
        {
            "exercise_id": "RUN_A",
            "elapsed_s": second,
            "distance_m": 4.0 * second,
            "hr_bpm": 140 if second < 125 else 160,
            "cadence_spm": 0 if second < 10 else 170,
        }
        for second in range(301)
    ]


def frames(spark: SparkSession) -> tuple[DataFrame, DataFrame, DataFrame]:
    return (
        silver_frame(spark, tables.EXERCISES, EXERCISES),
        silver_frame(spark, tables.EXERCISE_HR_ZONES, HR_ZONES),
        silver_frame(spark, tables.EXERCISE_SAMPLES, run_a_samples()),
    )


def runs_by_id(spark: SparkSession) -> dict[str, Any]:
    return {row["exercise_id"]: row for row in training.runs(*frames(spark)).collect()}


def test_runs_contain_only_running(spark: SparkSession) -> None:
    assert set(runs_by_id(spark)) == {"RUN_A", "RUN_B"}


def test_run_totals_and_pace(spark: SparkSession) -> None:
    run = runs_by_id(spark)["RUN_B"]

    assert run["run_date"].isoformat() == "2026-11-04"
    assert run["distance_km"] == 5.0
    assert run["pace_min_per_km"] == pytest.approx(6.0)
    assert run["perceived_load"] == 200.0


def test_cadence_ignores_standing_still(spark: SparkSession) -> None:
    assert runs_by_id(spark)["RUN_A"]["cadence_avg_spm"] == 170.0


def test_zone_time_and_shares(spark: SparkSession) -> None:
    runs = runs_by_id(spark)
    run_a, run_b = runs["RUN_A"], runs["RUN_B"]

    assert [run_a[f"zone_{zone}_s"] for zone in training.ZONES] == [600, 1200, 1200, 0, 0]
    assert [run_a[f"zone_{zone}_share"] for zone in training.ZONES] == pytest.approx(
        [0.2, 0.4, 0.4, 0.0, 0.0]
    )
    # No zone data (no heart rate): unknown, not zero.
    assert run_b["zone_1_s"] is None
    assert run_b["zone_1_share"] is None


def segments(spark: SparkSession, segment_m: int) -> dict[int, Any]:
    exercises, _, samples = frames(spark)
    return {
        row["segment"]: row
        for row in training.run_segments(exercises, samples, segment_m).collect()
    }


def test_kilometre_splits_with_a_partial_last_split(spark: SparkSession) -> None:
    splits = segments(spark, training.SPLIT_SEGMENT_M)

    assert set(splits) == {1, 2}
    first, last = splits[1], splits[2]
    assert (first["start_km"], first["distance_m"], first["duration_s"]) == (0.0, 1000.0, 250)
    assert (last["start_km"], last["distance_m"], last["duration_s"]) == (1.0, 200.0, 50)
    # Same speed, so the partial split has the same pace.
    assert first["pace_min_per_km"] == pytest.approx(250 / 60)
    assert last["pace_min_per_km"] == pytest.approx(250 / 60)
    assert (first["hr_avg_bpm"], first["hr_max_bpm"]) == (150.0, 160)
    assert first["cadence_avg_spm"] == 170.0
    assert first["run_date"].isoformat() == "2026-11-02"


def test_profile_in_100_m_segments(spark: SparkSession) -> None:
    profile = segments(spark, training.PROFILE_SEGMENT_M)

    assert sorted(profile) == list(range(1, 13))
    assert {(row["distance_m"], row["duration_s"]) for row in profile.values()} == {(100.0, 25)}
    assert profile[12]["start_km"] == pytest.approx(1.1)


def test_samples_without_distance_still_count_and_means_are_time_weighted(
    spark: SparkSession,
) -> None:
    samples = [
        # No GPS fix yet: before the first distance, so not in any segment.
        {"exercise_id": "RUN_B", "elapsed_s": 0, "hr_bpm": 90},
        # 10 s at 100 bpm with a distance.
        *(
            {"exercise_id": "RUN_B", "elapsed_s": s, "distance_m": 4.0 * s, "hr_bpm": 100}
            for s in range(1, 11)
        ),
        # Distance lost for 10 s (next sample only at 21 s), heart rate still recorded.
        {"exercise_id": "RUN_B", "elapsed_s": 11, "hr_bpm": 160},
        {"exercise_id": "RUN_B", "elapsed_s": 21, "distance_m": 84.0, "hr_bpm": 100},
    ]
    exercises = silver_frame(spark, tables.EXERCISES, EXERCISES)
    frame = silver_frame(spark, tables.EXERCISE_SAMPLES, samples)

    (segment,) = training.run_segments(exercises, frame, segment_m=1000).collect()

    assert (segment["duration_s"], segment["distance_m"]) == (20, 80.0)
    # 10 s at 100 bpm and 10 s at 160 bpm; a plain mean of the 11 samples would be ~105.
    assert segment["hr_avg_bpm"] == pytest.approx(130.0)
    assert segment["hr_max_bpm"] == 160


def test_running_per_week(spark: SparkSession) -> None:
    week = by_period(
        training.running_by_period(training.runs(*frames(spark))),
        "runs",
        "distance_km",
        "duration_s",
        "pace_min_per_km",
        "hr_avg_bpm",
        "running_index_avg",
        "cardio_load_sum",
        "perceived_load_avg",
        "zone_2_s",
        "zone_2_share",
    )[("week", "2026-11-02")]

    assert week == (
        2,
        15.0,
        4800.0,
        pytest.approx(4800 / 60 / 15),
        pytest.approx((140 * 3000 + 150 * 1800) / 4800),
        49.0,
        120.0,
        250.0,
        1200.0,
        pytest.approx(0.4),
    )


def test_other_sessions_per_activity(spark: SparkSession) -> None:
    exercises, _, _ = frames(spark)

    rows = {
        (row["granularity"], row["activity"]): (row["sessions"], row["duration_s"])
        for row in training.other_sessions_by_period(exercises).collect()
    }

    assert rows == {
        ("week", "STRENGTH_TRAINING"): (1, 3600.0),
        ("week", "CYCLING"): (1, 1200.0),
        ("month", "STRENGTH_TRAINING"): (1, 3600.0),
        ("month", "CYCLING"): (1, 1200.0),
    }
