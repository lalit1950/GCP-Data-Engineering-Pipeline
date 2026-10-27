"""
watermark_utils.py
-------------------
Implements batch watermarking so late-arriving records are never silently
dropped and re-runs stay idempotent.

Design:
- A Delta "control table" stores one row per (pipeline_stage, table_name):
      table_name | last_watermark | last_run_ts | updated_at
- On each run we don't just read "> last_watermark". We subtract a grace
  period (late_arrival_grace_minutes) to re-open a trailing window, so a
  record that arrives late (e.g. mobile app synced 2 hours after the event
  happened) still gets picked up on a later run instead of being lost.
- Because downstream writes (silver/gold) are MERGE-based upserts keyed on
  the business primary key, re-reading the same window repeatedly is safe:
  re-processing a record that hasn't changed is a no-op merge.
"""

from datetime import datetime, timedelta
from pyspark.sql import SparkSession, Row
from delta.tables import DeltaTable


def _control_table_exists(spark: SparkSession, control_table_path: str) -> bool:
    try:
        DeltaTable.forPath(spark, control_table_path)
        return True
    except Exception:
        return False


def init_control_table(spark: SparkSession, control_table_path: str) -> None:
    """Create the control table on first run if it doesn't exist yet."""
    if _control_table_exists(spark, control_table_path):
        return
    schema_df = spark.createDataFrame(
        [],
        schema="table_name STRING, last_watermark TIMESTAMP, last_run_ts TIMESTAMP",
    )
    schema_df.write.format("delta").mode("overwrite").save(control_table_path)


def get_last_watermark(
    spark: SparkSession,
    control_table_path: str,
    table_name: str,
    default_lookback_hours: int = 24,
) -> datetime:
    """
    Returns the last successfully processed watermark for a table.
    On first-ever run (no control row yet), defaults to `default_lookback_hours`
    ago so the initial load doesn't try to pull all history by accident.
    """
    init_control_table(spark, control_table_path)
    df = spark.read.format("delta").load(control_table_path)
    row = df.filter(df.table_name == table_name).collect()

    if not row:
        return datetime.utcnow() - timedelta(hours=default_lookback_hours)
    return row[0]["last_watermark"]


def compute_read_window(
    spark: SparkSession,
    control_table_path: str,
    table_name: str,
    late_arrival_grace_minutes: int,
    max_lookback_hours: int,
):
    """
    Returns (window_start, window_end) to read from the source/bronze layer.

    window_start = last_watermark - grace_period, capped by max_lookback_hours
                    so a long outage doesn't cause an unbounded full-table scan.
    window_end   = now (this batch's processing time)
    """
    last_watermark = get_last_watermark(spark, control_table_path, table_name)
    grace_start = last_watermark - timedelta(minutes=late_arrival_grace_minutes)
    hard_floor = datetime.utcnow() - timedelta(hours=max_lookback_hours)

    window_start = max(grace_start, hard_floor)
    window_end = datetime.utcnow()
    return window_start, window_end


def update_watermark(
    spark: SparkSession,
    control_table_path: str,
    table_name: str,
    new_watermark: datetime,
) -> None:
    """
    Advances the watermark to MAX(new_watermark, existing) — never move it
    backwards, and never move it past the max event_time actually observed
    in this batch (passed in by caller as `new_watermark`).
    """
    init_control_table(spark, control_table_path)
    delta_tbl = DeltaTable.forPath(spark, control_table_path)

    updates_df = spark.createDataFrame(
        [Row(table_name=table_name, last_watermark=new_watermark, last_run_ts=datetime.utcnow())]
    )

    (
        delta_tbl.alias("t")
        .merge(updates_df.alias("s"), "t.table_name = s.table_name")
        .whenMatchedUpdate(
            condition="s.last_watermark > t.last_watermark",
            set={
                "last_watermark": "s.last_watermark",
                "last_run_ts": "s.last_run_ts",
            },
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
