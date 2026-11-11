"""Bronze -> silver for Polar data.

Incremental: Auto Loader picks up only bronze files it has not processed yet (state is
kept in a checkpoint in a Unity Catalog volume). Idempotent: rows are MERGEd on their
keys, so reprocessing a file never duplicates data.

Resilient: a bronze file the parser cannot read goes to the 'rejected_records'
quarantine table instead of blocking the stream; the remaining files are processed,
the checkpoint advances, and the run then fails so the problem is noticed.

Run by the 'silver_polar' job (resources/silver_polar.yml) as a wheel entry point.
"""

import argparse
import logging
import re
import sys
import uuid
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from wearable_sync_pipelines.polar import parsing, tables

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.streaming import StreamingQuery

logger = logging.getLogger(__name__)

_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")

BatchHandler = Callable[["DataFrame", int], None]


class SilverRunError(RuntimeError):
    """The run finished, but some files were rejected or a stream failed."""


def main(argv: Sequence[str] | None = None) -> None:
    _configure_logging()
    args = _parse_args(argv)
    # Provided by Databricks serverless compute; never a wheel dependency.
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    schema = f"`{args.catalog}`.`{args.schema}`"
    run_id = str(uuid.uuid4())
    for table in tables.ALL_TABLES:
        _create_table_if_missing(spark, schema, table)

    bronze = args.bronze_path.rstrip("/")
    checkpoints = args.checkpoint_path.rstrip("/")
    # Both streams start before either is awaited: they run side by side, and a
    # failure in one does not stop the other.
    queries = {
        "exercise": _start_ingest(
            spark,
            source=f"{bronze}/exercise",
            checkpoint=f"{checkpoints}/exercise",
            handler=_exercise_handler(schema, run_id),
        ),
        "body_measurement": _start_ingest(
            spark,
            source=f"{bronze}/body_measurement",
            checkpoint=f"{checkpoints}/body_measurement",
            handler=_body_measurement_handler(schema, run_id),
        ),
    }
    problems = []
    for name, query in queries.items():
        try:
            query.awaitTermination()
        except Exception as error:  # noqa: BLE001 - reported together below
            logger.exception("Stream '%s' failed", name)
            problems.append(f"stream '{name}' failed: {error}")

    rejected = spark.sql(
        f"SELECT count(*) AS n FROM {schema}.`{tables.REJECTED_RECORDS.name}` "
        f"WHERE run_id = '{run_id}'"
    ).first()["n"]
    if rejected:
        problems.append(
            f"{rejected} bronze file(s) rejected; see {schema}.{tables.REJECTED_RECORDS.name} "
            f"WHERE run_id = '{run_id}'"
        )
    if problems:
        raise SilverRunError("; ".join(problems))
    logger.info("Silver Polar run %s finished", run_id)


def _configure_logging() -> None:
    # No-op when logging is already configured; needed in every process, including the
    # separate one that runs foreachBatch handlers on serverless (Spark Connect).
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bronze-path", required=True, help="e.g. /Volumes/<catalog>/polar/files")
    parser.add_argument("--catalog", required=True, type=_identifier, help="silver catalog")
    parser.add_argument("--schema", required=True, type=_identifier, help="silver schema")
    parser.add_argument("--checkpoint-path", required=True, help="Auto Loader checkpoints (volume)")
    return parser.parse_args(argv)


def _identifier(value: str) -> str:
    if not _IDENTIFIER.match(value):
        raise argparse.ArgumentTypeError(f"invalid Unity Catalog name: {value!r}")
    return value


def _create_table_if_missing(spark: "SparkSession", schema: str, table: tables.Table) -> None:
    # Existing tables are not altered here; new columns arrive through MERGE schema
    # evolution (see _merge).
    comment = table.comment.replace("'", "\\'")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {schema}.`{table.name}` ({table.ddl()}) COMMENT '{comment}'"
    )


def _start_ingest(
    spark: "SparkSession", *, source: str, checkpoint: str, handler: BatchHandler
) -> "StreamingQuery":
    from pyspark.sql import functions as F

    files = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        # One row per file: each bronze file is a single JSON document.
        .option("wholetext", "true")
        # Ignore anything that is not a finished record (e.g. leftover *.tmp files).
        .option("pathGlobFilter", "*.json")
        .load(source)
        .select(F.col("_metadata.file_path").alias("path"), F.col("value").alias("content"))
    )
    return (
        files.writeStream.foreachBatch(handler)
        .option("checkpointLocation", checkpoint)
        # Process everything new, then stop: the job is triggered, not continuous.
        .trigger(availableNow=True)
        .start()
    )


# foreachBatch functions run in a separate Python process on serverless (Spark Connect):
# they must use the batch's own session and capture only plain values, not 'spark'.


def _exercise_handler(schema: str, run_id: str) -> BatchHandler:
    def handle(batch: "DataFrame", batch_id: int) -> None:
        _configure_logging()
        exercises, zones, samples, rejected = [], [], [], []
        for file in batch.collect():
            try:
                envelope = parsing.load_envelope(file["content"], expected_data_type="exercise")
                parsed = parsing.parse_exercise(envelope, source_file=file["path"])
            # Parsing is pure Python: any error here means the record itself is bad.
            except Exception as error:  # noqa: BLE001
                rejected.append(_reject(file["path"], "exercise", error, run_id))
                continue
            exercises.append(parsed.exercise)
            zones.extend(parsed.hr_zones)
            samples.extend(parsed.samples)
        session = batch.sparkSession
        _merge(session, schema, tables.EXERCISES, exercises)
        _merge(session, schema, tables.EXERCISE_HR_ZONES, zones)
        _merge(session, schema, tables.EXERCISE_SAMPLES, samples)
        _merge(session, schema, tables.REJECTED_RECORDS, rejected)
        logger.info(
            "Batch %d: exercises=%d hr_zones=%d samples=%d rejected=%d",
            batch_id,
            len(exercises),
            len(zones),
            len(samples),
            len(rejected),
        )

    return handle


def _body_measurement_handler(schema: str, run_id: str) -> BatchHandler:
    def handle(batch: "DataFrame", batch_id: int) -> None:
        _configure_logging()
        rows, rejected = [], []
        for file in batch.collect():
            try:
                envelope = parsing.load_envelope(
                    file["content"], expected_data_type="body_measurement"
                )
                rows.append(parsing.parse_body_measurement(envelope, source_file=file["path"]))
            except Exception as error:  # noqa: BLE001
                rejected.append(_reject(file["path"], "body_measurement", error, run_id))
        session = batch.sparkSession
        _merge(session, schema, tables.BODY_MEASUREMENTS, rows)
        _merge(session, schema, tables.REJECTED_RECORDS, rejected)
        logger.info(
            "Batch %d: body_measurements=%d rejected=%d", batch_id, len(rows), len(rejected)
        )

    return handle


def _reject(path: str, data_type: str, error: Exception, run_id: str) -> dict[str, Any]:
    logger.warning("Rejected %s: %s", path, error)
    return parsing.rejected_record(
        source_file=path, data_type=data_type, error=error, run_id=run_id
    )


def last_per_key(rows: list[dict[str, Any]], keys: Sequence[str]) -> list[dict[str, Any]]:
    """One row per key (the last one wins): MERGE fails if several source rows match one target."""
    return list({tuple(row[key] for key in keys): row for row in rows}.values())


def _merge(
    spark: "SparkSession", schema: str, table: tables.Table, rows: list[dict[str, Any]]
) -> None:
    rows = last_per_key(rows, table.keys)
    if not rows:
        return
    ingested_at = datetime.now(UTC)
    data = [tuple(row[column] for column in table.row_columns) + (ingested_at,) for row in rows]
    source = f"silver_polar_{table.name}_batch"
    spark.createDataFrame(data, schema=table.ddl()).createOrReplaceTempView(source)
    on = " AND ".join(f"t.`{key}` = s.`{key}`" for key in table.keys)
    # Schema evolution: columns added to tables.py are added to the existing table.
    spark.sql(
        f"MERGE WITH SCHEMA EVOLUTION INTO {schema}.`{table.name}` AS t USING {source} AS s "
        f"ON {on} WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
    )
