import pytest

from wearable_sync_pipelines.gold import tables as gold_tables
from wearable_sync_pipelines.jobs import gold_polar

VALID_ARGS = [
    "--silver-catalog",
    "wearable_sync_silver",
    "--silver-schema",
    "polar",
    "--gold-catalog",
    "wearable_sync_gold",
    "--body-schema",
    "body",
    "--training-schema",
    "training",
    "--timezone",
    "Europe/Warsaw",
]


def test_arguments_are_parsed() -> None:
    args = gold_polar._parse_args(VALID_ARGS)

    assert (args.gold_catalog, args.body_schema, args.timezone) == (
        "wearable_sync_gold",
        "body",
        "Europe/Warsaw",
    )


@pytest.mark.parametrize("name", ["body; DROP TABLE x", "Body", "my-schema", ""])
def test_unsafe_identifiers_are_rejected(name: str) -> None:
    args = list(VALID_ARGS)
    args[args.index("body")] = name

    with pytest.raises(SystemExit):
        gold_polar._parse_args(args)


def test_gold_table_names_are_unique_per_schema() -> None:
    names = [(table.schema, table.name) for table in gold_tables.ALL_TABLES]

    assert len(names) == len(set(names))
