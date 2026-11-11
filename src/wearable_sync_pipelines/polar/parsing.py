"""Parse raw Polar records (bronze envelopes written by wearable_sync) into silver rows.

Pure Python with no Spark dependency, so the whole parsing logic is unit-tested
locally; the Spark job only reads files and writes the returned rows.

A bronze envelope looks like:
    {"source": "polar", "data_type": "exercise" | "body_measurement",
     "record_id": "...", "recorded_at": "<ISO 8601, UTC>", "payload": {<Polar API JSON>}}
"""

import json
import math
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

Row = dict[str, Any]

# Polar AccessLink exercise sample types used here (all others are ignored).
HEART_RATE = "0"  # bpm
SPEED = "1"  # km/h
CADENCE = "2"  # running cadence per foot (rpm); doubled to steps per minute
ALTITUDE = "3"  # m (barometric, low accuracy on some devices)
DISTANCE = "10"  # m, cumulative

# Below walking speed the pace is meaningless (standing, GPS noise).
MIN_SPEED_FOR_PACE_KMH = 1.0

_DURATION = re.compile(
    r"^PT(?:(?P<h>\d+(?:\.\d+)?)H)?(?:(?P<m>\d+(?:\.\d+)?)M)?(?:(?P<s>\d+(?:\.\d+)?)S)?$"
)


class InvalidRecordError(ValueError):
    """A bronze record does not have the structure this parser expects."""


@dataclass(frozen=True)
class ParsedExercise:
    exercise: Row
    hr_zones: list[Row]
    samples: list[Row]


def load_envelope(text: str, *, expected_data_type: str) -> dict[str, Any]:
    try:
        envelope = json.loads(text)
    except json.JSONDecodeError as error:
        raise InvalidRecordError(f"not valid JSON: {error}") from error
    for key in ("source", "data_type", "record_id", "payload"):
        if key not in envelope:
            raise InvalidRecordError(f"envelope is missing '{key}'")
    if envelope["source"] != "polar" or envelope["data_type"] != expected_data_type:
        raise InvalidRecordError(
            f"expected a polar/{expected_data_type} record, got "
            f"{envelope['source']}/{envelope['data_type']}"
        )
    return envelope


def parse_exercise(envelope: dict[str, Any], *, source_file: str) -> ParsedExercise:
    payload = envelope["payload"]
    exercise_id = _required(payload, "id")
    start_local = _parse_naive_datetime(_required(payload, "start_time"))
    offset_minutes = int(_required(payload, "start_time_utc_offset"))
    heart_rate = payload.get("heart_rate") or {}
    load = _training_load(payload.get("training_load_pro"))

    exercise = {
        "exercise_id": exercise_id,
        "sport": _required(payload, "sport"),
        "detailed_sport": payload.get("detailed_sport_info"),
        "start_time_local": start_local,
        "utc_offset_minutes": offset_minutes,
        "start_time_utc": (start_local - timedelta(minutes=offset_minutes)).replace(tzinfo=UTC),
        "duration_s": parse_duration_seconds(_required(payload, "duration")),
        "distance_m": _optional_float(payload.get("distance")),
        "calories": payload.get("calories"),
        "hr_avg_bpm": heart_rate.get("average"),
        "hr_max_bpm": heart_rate.get("maximum"),
        "running_index": payload.get("running_index"),
        "cardio_load": _optional_float(load.get("cardio-load")),
        "cardio_load_interpretation": load.get("cardio-load-interpretation"),
        "muscle_load": _optional_float(load.get("muscle-load")),
        "muscle_load_interpretation": load.get("muscle-load-interpretation"),
        "perceived_load": _optional_float(load.get("perceived-load")),
        "perceived_load_interpretation": load.get("perceived-load-interpretation"),
        "device": payload.get("device"),
        "has_route": payload.get("has_route"),
        "uploaded_at": _parse_aware_datetime(payload["upload_time"])
        if payload.get("upload_time")
        else None,
        "source_file": source_file,
    }
    return ParsedExercise(
        exercise=exercise,
        hr_zones=_parse_hr_zones(exercise_id, payload.get("heart_rate_zones") or []),
        samples=_parse_samples(exercise_id, payload.get("samples") or []),
    )


def parse_body_measurement(envelope: dict[str, Any], *, source_file: str) -> Row:
    """Body measurements only; personal attributes (birthday, gender) are left in bronze."""
    payload = envelope["payload"]
    return {
        "measurement_id": envelope["record_id"],
        "measured_at": _parse_aware_datetime(_required(payload, "modified")),
        "weight_kg": _optional_float(payload.get("weight")),
        "height_cm": _optional_float(payload.get("height")),
        "vo2max": payload.get("vo2_max"),
        "hr_max_bpm": payload.get("maximum_heart_rate"),
        "hr_rest_bpm": payload.get("resting_heart_rate"),
        "aerobic_threshold_bpm": payload.get("aerobic_threshold"),
        "anaerobic_threshold_bpm": payload.get("anaerobic_threshold"),
        "weight_source": payload.get("weight_source"),
        "source_file": source_file,
    }


def rejected_record(*, source_file: str, data_type: str, error: Exception, run_id: str) -> Row:
    """Quarantine row for a bronze file this parser cannot read."""
    return {
        "source_file": source_file,
        "data_type": data_type,
        "error": f"{type(error).__name__}: {error}"[:1000],
        "run_id": run_id,
    }


def parse_duration_seconds(value: str) -> float:
    """ISO 8601 durations as used by Polar: 'PT3771.649S', 'PT1H2M3S', 'PT5M10S'."""
    match = _DURATION.match(value)
    if match is None or value == "PT":
        raise InvalidRecordError(f"unsupported duration format: {value!r}")
    hours, minutes, seconds = (float(match.group(part) or 0) for part in ("h", "m", "s"))
    return hours * 3600 + minutes * 60 + seconds


def _training_load(value: Any) -> dict[str, Any]:
    """Polar returns an object; an absent value or an empty list means no load data."""
    if value is None or value == []:
        return {}
    if isinstance(value, dict):
        return value
    raise InvalidRecordError(f"unexpected training_load_pro shape: {type(value).__name__}")


def _parse_hr_zones(exercise_id: str, zones: list[dict[str, Any]]) -> list[Row]:
    return [
        {
            "exercise_id": exercise_id,
            # Polar indexes zones from 0; zones are conventionally numbered 1-5.
            "zone": int(zone["index"]) + 1,
            "lower_bpm": zone.get("lower_limit"),
            "upper_bpm": zone.get("upper_limit"),
            "time_in_zone_s": parse_duration_seconds(zone["in_zone"]) if zone.get("in_zone") else 0.0,
        }
        for zone in zones
    ]


def _parse_samples(exercise_id: str, samples: list[dict[str, Any]]) -> list[Row]:
    """Align the per-type sample series on a common elapsed-seconds axis."""
    by_elapsed: dict[int, Row] = {}
    columns = {
        HEART_RATE: "hr_bpm",
        SPEED: "speed_kmh",
        CADENCE: "cadence_spm",
        ALTITUDE: "altitude_m",
        DISTANCE: "distance_m",
    }
    for sample in samples:
        column = columns.get(str(sample.get("sample_type")))
        if column is None or not sample.get("data"):
            continue
        rate = int(sample.get("recording_rate") or 1)
        for index, token in enumerate(sample["data"].split(",")):
            value = _parse_sample_value(token)
            if value is None:
                continue
            row = by_elapsed.setdefault(index * rate, {})
            row[column] = value

    rows = []
    for elapsed_s in sorted(by_elapsed):
        values = by_elapsed[elapsed_s]
        speed = values.get("speed_kmh")
        cadence = values.get("cadence_spm")
        rows.append(
            {
                "exercise_id": exercise_id,
                "elapsed_s": elapsed_s,
                "hr_bpm": _optional_int(values.get("hr_bpm")),
                "speed_kmh": _optional_float(speed),
                "pace_min_per_km": 60.0 / speed
                if speed is not None and speed >= MIN_SPEED_FOR_PACE_KMH
                else None,
                # Polar reports running cadence per foot; steps per minute is the usual unit.
                "cadence_spm": int(cadence * 2) if cadence is not None else None,
                "altitude_m": _optional_float(values.get("altitude_m")),
                "distance_m": _optional_float(values.get("distance_m")),
            }
        )
    return rows


def _parse_sample_value(token: str) -> float | None:
    token = token.strip()
    if token == "" or token.upper() == "NULL":
        return None
    try:
        value = float(token)
    except ValueError as error:
        raise InvalidRecordError(f"unexpected sample value: {token!r}") from error
    return None if math.isnan(value) else value


def _required(payload: dict[str, Any], key: str) -> Any:
    value = payload.get(key)
    if value is None or value == "":
        raise InvalidRecordError(f"payload is missing '{key}'")
    return value


def _parse_naive_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is not None:
        raise InvalidRecordError(f"expected a local time without offset, got {value!r}")
    return parsed


def _parse_aware_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise InvalidRecordError(f"expected a time with offset, got {value!r}")
    return parsed.astimezone(UTC)


def _optional_float(value: Any) -> float | None:
    return None if value is None else float(value)


def _optional_int(value: Any) -> int | None:
    return None if value is None else int(value)
