import pytest

from wearable_sync_pipelines.jobs import silver_polar

VALID_ARGS = [
    "--bronze-path",
    "/Volumes/wearable_sync_bronze/polar/files",
    "--catalog",
    "wearable_sync_silver",
    "--schema",
    "polar",
    "--checkpoint-path",
    "/Volumes/wearable_sync_silver/polar/checkpoints",
]


def test_arguments_are_parsed() -> None:
    args = silver_polar._parse_args(VALID_ARGS)

    assert (args.catalog, args.schema) == ("wearable_sync_silver", "polar")


@pytest.mark.parametrize("name", ["polar; DROP TABLE x", "Polar", "my-schema", "a.b", ""])
def test_unsafe_identifiers_are_rejected(name: str) -> None:
    args = list(VALID_ARGS)
    args[args.index("polar")] = name

    with pytest.raises(SystemExit):
        silver_polar._parse_args(args)


def test_last_row_per_key_wins() -> None:
    rows = [
        {"exercise_id": "A", "elapsed_s": 0, "hr_bpm": 100},
        {"exercise_id": "A", "elapsed_s": 1, "hr_bpm": 110},
        {"exercise_id": "A", "elapsed_s": 0, "hr_bpm": 120},
    ]

    deduplicated = silver_polar.last_per_key(rows, ("exercise_id", "elapsed_s"))

    assert deduplicated == [
        {"exercise_id": "A", "elapsed_s": 0, "hr_bpm": 120},
        {"exercise_id": "A", "elapsed_s": 1, "hr_bpm": 110},
    ]
