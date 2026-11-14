"""Gold training metrics: runs, their distance profiles, and activity per period.

Runs are exercises with sport RUNNING. Dates come from the exercise's local start time
(TIMESTAMP_NTZ), so they do not depend on the Spark session time zone. Paces are
minutes per kilometre, always computed as time / distance (never averaged).
"""

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from wearable_sync_pipelines.gold.periods import WEEK_MONTH, aggregate_by_period

RUNNING = "RUNNING"
ZONES = (1, 2, 3, 4, 5)

PROFILE_SEGMENT_M = 100
SPLIT_SEGMENT_M = 1000


def _pace(duration_s: Column, distance_m: Column) -> Column:
    """Minutes per kilometre; NULL without a positive distance."""
    return F.when(distance_m > 0, (duration_s / 60) / (distance_m / 1000))


def _cadence(cadence_spm: Column) -> Column:
    # Zero cadence means standing still (e.g. at a crossing); it would drag the mean down.
    return F.when(cadence_spm > 0, cadence_spm)


def _time_weighted(value: Column, weight_s: Column) -> Column:
    """Mean of 'value' weighted by time, over the samples where it is known."""
    return F.sum(F.when(value.isNotNull(), value * weight_s)) / F.sum(
        F.when(value.isNotNull(), weight_s)
    )


def _running(exercises: DataFrame) -> DataFrame:
    return exercises.where(F.col("sport") == RUNNING)


def _zone_columns(suffix: str) -> list[str]:
    return [f"zone_{zone}_{suffix}" for zone in ZONES]


def _with_zone_shares(frame: DataFrame) -> DataFrame:
    """Share (0-1) of the time in zones 1-5 spent in each zone; NULL without zone data."""
    total = sum((F.col(column) for column in _zone_columns("s")), F.lit(0.0))
    for zone in ZONES:
        frame = frame.withColumn(
            f"zone_{zone}_share", F.when(total > 0, F.col(f"zone_{zone}_s") / total)
        )
    return frame


def runs(exercises: DataFrame, hr_zones: DataFrame, samples: DataFrame) -> DataFrame:
    """One row per run with its totals, Polar's indicators and time in heart rate zones."""
    running = _running(exercises)
    zones = (
        hr_zones.groupBy("exercise_id")
        .pivot("zone", list(ZONES))
        .agg(F.sum("time_in_zone_s"))
        .toDF("exercise_id", *_zone_columns("s"))
        # A run with zone data but no time in some zone spent 0 s there; a run without
        # zone data at all (no heart rate) keeps NULLs after the outer join below.
        .fillna(0.0)
    )
    # Only the samples of runs: other sessions have as many samples, all unused here.
    cadence = (
        samples.join(running.select("exercise_id"), "exercise_id", "left_semi")
        .groupBy("exercise_id")
        .agg(F.avg(_cadence(F.col("cadence_spm"))).alias("cadence_avg_spm"))
    )
    frame = (
        running.join(zones, "exercise_id", "left")
        .join(cadence, "exercise_id", "left")
        .select(
            "exercise_id",
            F.to_date("start_time_local").alias("run_date"),
            "start_time_local",
            (F.col("distance_m") / 1000).alias("distance_km"),
            "duration_s",
            _pace(F.col("duration_s"), F.col("distance_m")).alias("pace_min_per_km"),
            "hr_avg_bpm",
            "hr_max_bpm",
            "cadence_avg_spm",
            "running_index",
            "cardio_load",
            "perceived_load",
            *_zone_columns("s"),
        )
    )
    return _with_zone_shares(frame)


def run_segments(exercises: DataFrame, samples: DataFrame, segment_m: int) -> DataFrame:
    """Each run cut into consecutive distance segments of 'segment_m' metres.

    Each sample is charged with the time and distance until the next sample, so a
    segment's duration and distance are what was actually covered in it: the last,
    partial segment of a run reports its real (shorter) distance and a comparable pace.
    A step that crosses a segment boundary counts fully towards the earlier segment, so
    segment lengths are accurate to one sampling step (3-4 m at 1 s and running pace).

    A sample without a distance (e.g. no GPS fix) keeps the last known distance, so its
    heart rate and cadence still count; samples before the first distance are dropped.
    Heart rate and cadence means are weighted by each sample's time.
    """
    run_ids = _running(exercises).select(
        "exercise_id", F.to_date("start_time_local").alias("run_date"), "start_time_local"
    )
    ordered = Window.partitionBy("exercise_id").orderBy("elapsed_s")
    so_far = ordered.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    steps = (
        samples.join(run_ids, "exercise_id")
        .withColumn("distance_m", F.last("distance_m", ignorenulls=True).over(so_far))
        .withColumn("step_s", F.lead("elapsed_s").over(ordered) - F.col("elapsed_s"))
        .withColumn("step_m", F.lead("distance_m").over(ordered) - F.col("distance_m"))
        # The last sample of a run has no next one, so nothing to charge it with.
        .where(F.col("step_s").isNotNull() & F.col("distance_m").isNotNull())
        .withColumn("segment", F.floor(F.col("distance_m") / segment_m).cast("int") + 1)
    )
    return (
        steps.groupBy("exercise_id", "run_date", "start_time_local", "segment")
        .agg(
            F.sum("step_m").alias("distance_m"),
            F.sum("step_s").alias("duration_s"),
            _time_weighted(F.col("hr_bpm"), F.col("step_s")).alias("hr_avg_bpm"),
            F.max("hr_bpm").alias("hr_max_bpm"),
            _time_weighted(_cadence(F.col("cadence_spm")), F.col("step_s")).alias(
                "cadence_avg_spm"
            ),
        )
        .select(
            "exercise_id",
            "run_date",
            "start_time_local",
            "segment",
            ((F.col("segment") - 1) * segment_m / 1000).alias("start_km"),
            "distance_m",
            "duration_s",
            _pace(F.col("duration_s"), F.col("distance_m")).alias("pace_min_per_km"),
            "hr_avg_bpm",
            "hr_max_bpm",
            "cadence_avg_spm",
        )
    )


def running_by_period(runs_frame: DataFrame) -> DataFrame:
    """Running volume and averages per week and month."""
    with_hr = F.col("hr_avg_bpm").isNotNull()
    frame = aggregate_by_period(
        runs_frame,
        date_column="run_date",
        granularities=WEEK_MONTH,
        aggregations=[
            F.count("*").alias("runs"),
            F.sum("distance_km").alias("distance_km"),
            F.sum("duration_s").alias("duration_s"),
            # Pace over runs with a distance, from their total time and distance.
            F.sum(F.when(F.col("distance_km") > 0, F.col("duration_s"))).alias("_paced_s"),
            # Heart rate weighted by duration: a long run counts more than a short one.
            (
                F.sum(F.when(with_hr, F.col("hr_avg_bpm") * F.col("duration_s")))
                / F.sum(F.when(with_hr, F.col("duration_s")))
            ).alias("hr_avg_bpm"),
            F.avg("running_index").alias("running_index_avg"),
            F.sum("cardio_load").alias("cardio_load_sum"),
            F.avg("perceived_load").alias("perceived_load_avg"),
            *(F.sum(column).alias(column) for column in _zone_columns("s")),
        ],
    )
    frame = frame.withColumn(
        "pace_min_per_km", _pace(F.col("_paced_s"), F.col("distance_km") * 1000)
    ).drop("_paced_s")
    return _with_zone_shares(frame)


def other_sessions_by_period(exercises: DataFrame) -> DataFrame:
    """Number and total duration of non-running sessions per week and month, by activity.

    The activity is Polar's detailed sport (e.g. STRENGTH_TRAINING, which Polar files
    under sport OTHER), or the sport when there is no detailed one.
    """
    sessions = exercises.where(F.col("sport") != RUNNING).select(
        F.to_date("start_time_local").alias("date"),
        F.coalesce("detailed_sport", "sport").alias("activity"),
        "duration_s",
    )
    return aggregate_by_period(
        sessions,
        date_column="date",
        granularities=WEEK_MONTH,
        group_by=["activity"],
        aggregations=[
            F.count("*").alias("sessions"),
            F.sum("duration_s").alias("duration_s"),
        ],
    )
