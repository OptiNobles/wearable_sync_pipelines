"""Silver DataFrames for gold tests, built with the real silver table schemas."""

from typing import Any

from pyspark.sql import DataFrame, SparkSession

from wearable_sync_pipelines.polar import tables


def silver_frame(spark: SparkSession, table: tables.Table, rows: list[dict[str, Any]]) -> DataFrame:
    """A DataFrame with exactly the silver table's columns; omitted columns are NULL."""
    unknown = {column for row in rows for column in row} - set(table.column_names)
    if unknown:
        raise ValueError(f"not columns of {table.name}: {sorted(unknown)}")
    data = [tuple(row.get(column) for column in table.column_names) for row in rows]
    return spark.createDataFrame(data, schema=table.ddl())


def by_period(frame: DataFrame, *columns: str) -> dict[tuple[str, str], tuple[Any, ...]]:
    """Rows keyed by (granularity, ISO period start), for easy assertions."""
    return {
        (row["granularity"], row["period_start"].isoformat()): tuple(row[c] for c in columns)
        for row in frame.collect()
    }
