import pytest

from polar_records import body_envelope, exercise_envelope
from wearable_sync_pipelines.polar import parsing, tables

SOURCE = "/Volumes/example/polar/files/x.json"


def parsed_rows() -> dict[str, list[dict[str, object]]]:
    run = parsing.parse_exercise(exercise_envelope(), source_file=SOURCE)
    return {
        "exercises": [run.exercise],
        "exercise_hr_zones": run.hr_zones,
        "exercise_samples": run.samples,
        "body_measurements": [parsing.parse_body_measurement(body_envelope(), source_file=SOURCE)],
        "rejected_records": [
            parsing.rejected_record(
                source_file=SOURCE,
                data_type="exercise",
                error=parsing.InvalidRecordError("bad"),
                run_id="run-1",
            )
        ],
    }


@pytest.mark.parametrize("table", tables.ALL_TABLES, ids=lambda table: table.name)
def test_parser_rows_match_table_columns(table: tables.Table) -> None:
    for row in parsed_rows()[table.name]:
        assert tuple(row) == table.row_columns


@pytest.mark.parametrize("table", tables.ALL_TABLES, ids=lambda table: table.name)
def test_keys_are_columns_and_ingestion_time_is_last(table: tables.Table) -> None:
    assert set(table.keys) <= set(table.column_names)
    assert table.column_names[-1] == tables.INGESTED_AT


def test_ddl_marks_keys_not_null() -> None:
    ddl = tables.EXERCISE_SAMPLES.ddl()

    assert "exercise_id STRING NOT NULL" in ddl
    assert "elapsed_s INT NOT NULL" in ddl
    assert "hr_bpm INT," in ddl
