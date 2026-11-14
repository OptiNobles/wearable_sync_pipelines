"""Silver -> gold for Polar data.

Full recompute: every gold table is rebuilt from the whole of silver on every run and
replaced in one step (CREATE OR REPLACE keeps the table's history and grants). At this
data volume a full rebuild costs the same as an incremental one, and corrections in
silver (e.g. a perceived load added after a run) always reach gold.

Local dates: the Spark session time zone is set to --timezone, so to_date() of a UTC
timestamp gives the local calendar date. Spark validates the name when it is set, so an
unknown zone fails the task before any table is touched.

Run by the 'gold' task of the 'wearable_sync_polar' job (resources/polar.yml) as a
wheel entry point.
"""

import argparse
import logging
from collections.abc import Sequence
from typing import TYPE_CHECKING

from wearable_sync_pipelines.jobs.cli import configure_logging, identifier
from wearable_sync_pipelines.polar import tables as silver_tables

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from wearable_sync_pipelines.gold.tables import Silver

logger = logging.getLogger(__name__)


def main(argv: Sequence[str] | None = None) -> None:
    configure_logging()
    args = _parse_args(argv)
    # PySpark (and the gold modules that need it) is provided by Databricks serverless
    # compute; it is never a wheel dependency.
    from pyspark.sql import SparkSession

    from wearable_sync_pipelines.gold import tables as gold_tables

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", args.timezone)
    silver = _read_silver(spark, f"`{args.silver_catalog}`.`{args.silver_schema}`")
    schemas = {"body": args.body_schema}

    for table in gold_tables.ALL_TABLES:
        target = f"`{args.gold_catalog}`.`{schemas[table.schema]}`.`{table.name}`"
        _replace_table(table.build(silver), target, table.comment)
        logger.info("Replaced %s", target)


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--silver-catalog", required=True, type=identifier)
    parser.add_argument("--silver-schema", required=True, type=identifier)
    parser.add_argument("--gold-catalog", required=True, type=identifier)
    parser.add_argument("--body-schema", required=True, type=identifier)
    parser.add_argument("--timezone", required=True, help="IANA name, e.g. Europe/Warsaw")
    return parser.parse_args(argv)


def _read_silver(spark: "SparkSession", schema: str) -> "Silver":
    from wearable_sync_pipelines.gold.tables import Silver

    def table(definition: silver_tables.Table) -> "DataFrame":
        return spark.table(f"{schema}.`{definition.name}`")

    return Silver(
        exercises=table(silver_tables.EXERCISES),
        exercise_hr_zones=table(silver_tables.EXERCISE_HR_ZONES),
        exercise_samples=table(silver_tables.EXERCISE_SAMPLES),
        body_measurements=table(silver_tables.BODY_MEASUREMENTS),
    )


def _replace_table(frame: "DataFrame", target: str, comment: str) -> None:
    # The comment is part of the same CREATE OR REPLACE: one atomic commit, one version
    # in the table history per run.
    frame.writeTo(target).using("delta").tableProperty("comment", comment).createOrReplace()
