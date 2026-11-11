from datetime import UTC, datetime

import pytest

from polar_records import as_text, body_envelope, exercise_envelope, strength_envelope
from wearable_sync_pipelines.polar import parsing
from wearable_sync_pipelines.polar.parsing import InvalidRecordError

SOURCE = "/Volumes/wearable_sync_bronze/polar/files/exercise/2026-11-02/EXAMPLE_RUN_1.json"


def parse_run(**overrides: object) -> parsing.ParsedExercise:
    return parsing.parse_exercise(exercise_envelope(**overrides), source_file=SOURCE)


# --- envelope -----------------------------------------------------------------


def test_envelope_is_loaded_from_json_text() -> None:
    envelope = parsing.load_envelope(as_text(exercise_envelope()), expected_data_type="exercise")

    assert envelope["record_id"] == "EXAMPLE_RUN_1"


@pytest.mark.parametrize("text", ["not json", "{}", '{"source": "polar"}'])
def test_malformed_envelope_is_rejected(text: str) -> None:
    with pytest.raises(InvalidRecordError):
        parsing.load_envelope(text, expected_data_type="exercise")


def test_envelope_of_another_data_type_is_rejected() -> None:
    with pytest.raises(InvalidRecordError, match="polar/exercise"):
        parsing.load_envelope(as_text(body_envelope()), expected_data_type="exercise")


# --- durations ----------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "seconds"),
    [("PT3771.649S", 3771.649), ("PT1H2M3S", 3723.0), ("PT5M10S", 310.0), ("PT8H", 28800.0)],
)
def test_iso_durations_are_parsed(value: str, seconds: float) -> None:
    assert parsing.parse_duration_seconds(value) == pytest.approx(seconds)


@pytest.mark.parametrize("value", ["PT", "1H", "P1D", "PT-5S", ""])
def test_unsupported_durations_are_rejected(value: str) -> None:
    with pytest.raises(InvalidRecordError):
        parsing.parse_duration_seconds(value)


# --- exercise -----------------------------------------------------------------


def test_exercise_summary_fields() -> None:
    exercise = parse_run().exercise

    assert exercise["exercise_id"] == "EXAMPLE_RUN_1"
    assert exercise["sport"] == "RUNNING"
    assert exercise["duration_s"] == 3750.0
    assert exercise["distance_m"] == 10000.0
    assert (exercise["hr_avg_bpm"], exercise["hr_max_bpm"]) == (140, 165)
    assert exercise["cardio_load"] == 85.5
    assert exercise["running_index"] == 50
    assert exercise["source_file"] == SOURCE


def test_start_time_is_kept_local_and_converted_to_utc() -> None:
    exercise = parse_run().exercise

    assert exercise["start_time_local"] == datetime(2026, 11, 2, 7, 2, 18)
    assert exercise["start_time_utc"] == datetime(2026, 11, 2, 6, 2, 18, tzinfo=UTC)
    assert exercise["utc_offset_minutes"] == 60


def test_strength_session_has_no_running_fields() -> None:
    exercise = parsing.parse_exercise(strength_envelope(), source_file=SOURCE).exercise

    assert exercise["detailed_sport"] == "STRENGTH_TRAINING"
    assert exercise["distance_m"] is None
    assert exercise["running_index"] is None


@pytest.mark.parametrize("missing", ["id", "sport", "start_time", "duration"])
def test_exercise_without_required_field_is_rejected(missing: str) -> None:
    envelope = exercise_envelope()
    del envelope["payload"][missing]

    with pytest.raises(InvalidRecordError, match=missing):
        parsing.parse_exercise(envelope, source_file=SOURCE)


@pytest.mark.parametrize("value", [None, []])
def test_missing_training_load_gives_empty_load_fields(value: object) -> None:
    exercise = parse_run(training_load_pro=value).exercise

    assert exercise["cardio_load"] is None
    assert exercise["cardio_load_interpretation"] is None


@pytest.mark.parametrize("value", [[{"cardio-load": 40.0}], "HIGH", 42])
def test_unexpected_training_load_shape_is_rejected(value: object) -> None:
    with pytest.raises(InvalidRecordError, match="training_load_pro"):
        parse_run(training_load_pro=value)


def test_route_never_reaches_silver_rows() -> None:
    parsed = parse_run()
    all_keys = set(parsed.exercise) | {key for row in parsed.samples for key in row}

    assert not {"route", "latitude", "longitude"} & all_keys


# --- heart rate zones ---------------------------------------------------------


def test_hr_zones_are_numbered_from_one_with_seconds() -> None:
    zones = parse_run().hr_zones

    assert [(zone["zone"], zone["time_in_zone_s"]) for zone in zones] == [
        (1, 120.0),
        (2, 310.0),
        (3, 3600.0),
    ]
    assert zones[0]["lower_bpm"] == 95


# --- samples ------------------------------------------------------------------


def test_samples_are_aligned_on_elapsed_seconds() -> None:
    samples = parse_run().samples

    assert [sample["elapsed_s"] for sample in samples] == [0, 1, 2, 3]
    assert samples[1] == {
        "exercise_id": "EXAMPLE_RUN_1",
        "elapsed_s": 1,
        "hr_bpm": 121,
        "speed_kmh": 10.0,
        "pace_min_per_km": 6.0,
        "cadence_spm": 168,
        "altitude_m": 100.5,
        "distance_m": 2.8,
    }


def test_missing_sample_values_become_null() -> None:
    assert parse_run().samples[2]["hr_bpm"] is None


def test_pace_is_null_when_standing_still() -> None:
    assert parse_run().samples[0]["pace_min_per_km"] is None


def test_cadence_is_converted_from_per_foot_to_steps_per_minute() -> None:
    assert [sample["cadence_spm"] for sample in parse_run().samples] == [0, 168, 170, 172]


def test_recording_rate_spaces_samples_in_time() -> None:
    samples = parse_run(
        samples=[{"sample_type": "0", "recording_rate": 5, "data": "120,130,140"}]
    ).samples

    assert [(sample["elapsed_s"], sample["hr_bpm"]) for sample in samples] == [
        (0, 120),
        (5, 130),
        (10, 140),
    ]


def test_invalid_sample_value_is_rejected() -> None:
    with pytest.raises(InvalidRecordError, match="sample value"):
        parse_run(samples=[{"sample_type": "0", "recording_rate": 1, "data": "120,abc"}])


# --- body measurements --------------------------------------------------------


def test_body_measurement_fields() -> None:
    row = parsing.parse_body_measurement(body_envelope(), source_file=SOURCE)

    assert row["measurement_id"] == "20261108T080232Z"
    assert row["measured_at"] == datetime(2026, 11, 8, 8, 2, 32, tzinfo=UTC)
    assert (row["weight_kg"], row["vo2max"], row["hr_rest_bpm"]) == (80.0, 50, 55)


def test_personal_attributes_stay_out_of_silver() -> None:
    row = parsing.parse_body_measurement(body_envelope(), source_file=SOURCE)

    assert not {"birthday", "gender"} & set(row)


def test_rejected_record_keeps_file_error_and_run() -> None:
    row = parsing.rejected_record(
        source_file=SOURCE,
        data_type="exercise",
        error=InvalidRecordError("payload is missing 'id'"),
        run_id="run-1",
    )

    assert row == {
        "source_file": SOURCE,
        "data_type": "exercise",
        "error": "InvalidRecordError: payload is missing 'id'",
        "run_id": "run-1",
    }


def test_rejected_record_error_is_truncated() -> None:
    row = parsing.rejected_record(
        source_file=SOURCE, data_type="exercise", error=ValueError("x" * 5000), run_id="r"
    )

    assert len(row["error"]) == 1000


def test_body_measurement_time_is_normalized_to_utc() -> None:
    row = parsing.parse_body_measurement(
        body_envelope(modified="2026-11-08T09:02:32+01:00"), source_file=SOURCE
    )

    assert row["measured_at"] == datetime(2026, 11, 8, 8, 2, 32, tzinfo=UTC)
