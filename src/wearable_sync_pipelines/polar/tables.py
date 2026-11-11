"""Silver table definitions for Polar data: columns, types and merge keys.

The parser (parsing.py) produces rows with exactly these columns except
'ingested_at', which the job adds; tests keep the two in sync.
"""

from dataclasses import dataclass

INGESTED_AT = "ingested_at"


@dataclass(frozen=True)
class Table:
    name: str
    columns: tuple[tuple[str, str], ...]
    keys: tuple[str, ...]
    comment: str

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(name for name, _ in self.columns)

    @property
    def row_columns(self) -> tuple[str, ...]:
        """Columns the parser fills (everything but the ingestion timestamp)."""
        return tuple(name for name in self.column_names if name != INGESTED_AT)

    def ddl(self) -> str:
        """Column definitions for CREATE TABLE and as a Spark DataFrame schema."""
        return ", ".join(
            f"{name} {sql_type}{' NOT NULL' if name in self.keys else ''}"
            for name, sql_type in self.columns
        )


EXERCISES = Table(
    name="exercises",
    keys=("exercise_id",),
    comment="One row per Polar exercise (all sports).",
    columns=(
        ("exercise_id", "STRING"),
        ("sport", "STRING"),
        ("detailed_sport", "STRING"),
        ("start_time_local", "TIMESTAMP_NTZ"),
        ("utc_offset_minutes", "INT"),
        ("start_time_utc", "TIMESTAMP"),
        ("duration_s", "DOUBLE"),
        ("distance_m", "DOUBLE"),
        ("calories", "INT"),
        ("hr_avg_bpm", "INT"),
        ("hr_max_bpm", "INT"),
        ("running_index", "INT"),
        ("cardio_load", "DOUBLE"),
        ("cardio_load_interpretation", "STRING"),
        ("muscle_load", "DOUBLE"),
        ("muscle_load_interpretation", "STRING"),
        ("perceived_load", "DOUBLE"),
        ("perceived_load_interpretation", "STRING"),
        ("device", "STRING"),
        ("has_route", "BOOLEAN"),
        ("uploaded_at", "TIMESTAMP"),
        ("source_file", "STRING"),
        (INGESTED_AT, "TIMESTAMP"),
    ),
)

EXERCISE_HR_ZONES = Table(
    name="exercise_hr_zones",
    keys=("exercise_id", "zone"),
    comment="Time spent in each heart rate zone (1-5) per exercise, as computed by Polar.",
    columns=(
        ("exercise_id", "STRING"),
        ("zone", "INT"),
        ("lower_bpm", "INT"),
        ("upper_bpm", "INT"),
        ("time_in_zone_s", "DOUBLE"),
        (INGESTED_AT, "TIMESTAMP"),
    ),
)

EXERCISE_SAMPLES = Table(
    name="exercise_samples",
    keys=("exercise_id", "elapsed_s"),
    comment="Per-second exercise samples aligned on elapsed time (no GPS route).",
    columns=(
        ("exercise_id", "STRING"),
        ("elapsed_s", "INT"),
        ("hr_bpm", "INT"),
        ("speed_kmh", "DOUBLE"),
        ("pace_min_per_km", "DOUBLE"),
        ("cadence_spm", "INT"),
        ("altitude_m", "DOUBLE"),
        ("distance_m", "DOUBLE"),
        (INGESTED_AT, "TIMESTAMP"),
    ),
)

BODY_MEASUREMENTS = Table(
    name="body_measurements",
    keys=("measurement_id",),
    comment="Polar physical information snapshots (weight, VO2max, heart rate limits).",
    columns=(
        ("measurement_id", "STRING"),
        ("measured_at", "TIMESTAMP"),
        ("weight_kg", "DOUBLE"),
        ("height_cm", "DOUBLE"),
        ("vo2max", "INT"),
        ("hr_max_bpm", "INT"),
        ("hr_rest_bpm", "INT"),
        ("aerobic_threshold_bpm", "INT"),
        ("anaerobic_threshold_bpm", "INT"),
        ("weight_source", "STRING"),
        ("source_file", "STRING"),
        (INGESTED_AT, "TIMESTAMP"),
    ),
)

REJECTED_RECORDS = Table(
    name="rejected_records",
    keys=("source_file",),
    comment="Bronze files that could not be parsed (quarantine); the rest of the data keeps flowing.",
    columns=(
        ("source_file", "STRING"),
        ("data_type", "STRING"),
        ("error", "STRING"),
        ("run_id", "STRING"),
        (INGESTED_AT, "TIMESTAMP"),
    ),
)

ALL_TABLES = (EXERCISES, EXERCISE_HR_ZONES, EXERCISE_SAMPLES, BODY_MEASUREMENTS, REJECTED_RECORDS)
