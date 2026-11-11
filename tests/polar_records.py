"""Fictional bronze records for tests (never real personal data)."""

import json
from typing import Any


def exercise_envelope(**payload_overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "id": "EXAMPLE_RUN_1",
        "upload_time": "2026-11-02T08:15:00Z",
        "device": "Example Watch",
        "start_time": "2026-11-02T07:02:18",
        "start_time_utc_offset": 60,
        "duration": "PT1H2M30S",
        "calories": 700,
        "distance": 10000.0,
        "heart_rate": {"average": 140, "maximum": 165},
        "training_load_pro": {
            "cardio-load": 85.5,
            "cardio-load-interpretation": "MEDIUM",
            "muscle-load": 1200.0,
            "muscle-load-interpretation": "MEDIUM",
            "perceived-load": 300.0,
            "perceived-load-interpretation": "MEDIUM",
            "user-rpe": "UNKNOWN",
        },
        "sport": "RUNNING",
        "has_route": True,
        "detailed_sport_info": "RUNNING",
        "running_index": 50,
        "heart_rate_zones": [
            {"index": 0, "lower_limit": 95, "upper_limit": 114, "in_zone": "PT120S"},
            {"index": 1, "lower_limit": 114, "upper_limit": 133, "in_zone": "PT5M10S"},
            {"index": 2, "lower_limit": 133, "upper_limit": 152, "in_zone": "PT1H0M0S"},
        ],
        "samples": [
            {"sample_type": "0", "recording_rate": 1, "data": "120,121,NULL,125"},
            {"sample_type": "1", "recording_rate": 1, "data": "0.0,10.0,12.0,12.0"},
            {"sample_type": "2", "recording_rate": 1, "data": "0,84,85,86"},
            {"sample_type": "3", "recording_rate": 1, "data": "100.0,100.5,101.0,101.5"},
            {"sample_type": "10", "recording_rate": 1, "data": "0.0,2.8,6.1,9.4"},
            {"sample_type": "99", "recording_rate": 1, "data": "1,2,3,4"},
        ],
        # Route data must never reach silver.
        "route": [{"latitude": 0.0, "longitude": 0.0, "time": "PT0S"}],
    }
    payload.update(payload_overrides)
    return {
        "source": "polar",
        "data_type": "exercise",
        "record_id": payload["id"],
        "recorded_at": "2026-11-02T06:02:18Z",
        "payload": payload,
    }


def strength_envelope() -> dict[str, Any]:
    envelope = exercise_envelope(
        id="EXAMPLE_GYM_1",
        sport="OTHER",
        detailed_sport_info="STRENGTH_TRAINING",
        distance=None,
        running_index=None,
        has_route=False,
        samples=[{"sample_type": "0", "recording_rate": 1, "data": "90,95"}],
    )
    del envelope["payload"]["distance"]
    del envelope["payload"]["running_index"]
    return envelope


def body_envelope(**payload_overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "weight": 80.0,
        "height": 180.0,
        "created": "2026-09-01T10:00:00Z",
        "modified": "2026-11-08T08:02:32Z",
        "birthday": "1990-01-01",
        "gender": "MALE",
        "maximum_heart_rate": 190,
        "resting_heart_rate": 55,
        "aerobic_threshold": 140,
        "anaerobic_threshold": 170,
        "vo2_max": 50,
        "weight_source": "SOURCE_USER",
        "training_background": "REGULAR",
        "typical_day": "MOSTLY_SITTING",
        "sleep_goal": "PT8H",
    }
    payload.update(payload_overrides)
    return {
        "source": "polar",
        "data_type": "body_measurement",
        "record_id": "20261108T080232Z",
        "recorded_at": "2026-11-08T08:02:32Z",
        "payload": payload,
    }


def as_text(envelope: dict[str, Any]) -> str:
    return json.dumps(envelope)
