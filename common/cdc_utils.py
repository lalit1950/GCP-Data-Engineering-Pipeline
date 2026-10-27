"""
cdc_utils.py
------------
Change Data Capture helpers.

Two supported CDC modes:

1) native_cdc (preferred): source already emits op_type ('I'/'U'/'D') and a
   source_ts per row (e.g. Debezium, AWS DMS, native DB CDC logs). We just
   read incrementally and merge.

2) snapshot_diff (fallback): source has no CDC feed, only full/incremental
   snapshots. We derive an op_type by hashing rows and comparing against the
   previous snapshot's row hashes (classic "hash diffing" SCD technique).

Both paths converge on the same `apply_cdc_merge()` function that writes
into a Delta table using MERGE, which is what makes the whole pipeline
idempotent and re-runnable.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable


def read_incremental_jdbc(
    spark: SparkSession,
    jdbc_url: str,
    jdbc_properties: dict,
    source_table: str,
    event_time_col: str,
    window_start,
    window_end,
) -> DataFrame:
    """
    Pulls only the rows changed in [window_start, window_end] using a
    predicate pushdown on the event-time column. This is the "extract" half
    of native CDC ingestion — the source table/view is expected to expose
    op_type and source_ts (see config.py docstring).
    """
    predicate = (
        f"{event_time_col} >= '{window_start.isoformat()}' "
        f"AND {event_time_col} < '{window_end.isoformat()}'"
    )
    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", f"(SELECT * FROM {source_table} WHERE {predicate}) t")
        .options(**jdbc_properties)
        .load()
    )
    return df


def derive_cdc_from_snapshot(
    current_snapshot: DataFrame,
    previous_snapshot: DataFrame,
    primary_keys: list,
) -> DataFrame:
    """
    Fallback CDC derivation when the source has no native change feed.
    Hashes all non-key columns and compares snapshots to synthesize
    op_type = I (new key), U (key exists, hash changed), D (key missing now).
    """
    compare_cols = [c for c in current_snapshot.columns if c not in primary_keys]

    def with_hash(df):
        return df.withColumn("_row_hash", F.sha2(F.concat_ws("||", *compare_cols), 256))

    curr = with_hash(current_snapshot)
    prev = with_hash(previous_snapshot).select(
        *primary_keys, F.col("_row_hash").alias("_prev_hash")
    )

    joined = curr.join(prev, on=primary_keys, how="full_outer")

    result = joined.withColumn(
        "op_type",
        F.when(F.col("_prev_hash").isNull(), F.lit("I"))
        .when(F.col("_row_hash").isNull(), F.lit("D"))
        .when(F.col("_row_hash") != F.col("_prev_hash"), F.lit("U"))
        .otherwise(F.lit(None)),
    ).filter(F.col("op_type").isNotNull())

    return result.drop("_row_hash", "_prev_hash").withColumn("source_ts", F.current_timestamp())


def dedup_latest_change_per_key(
    df: DataFrame, primary_keys: list, order_col: str = "source_ts"
) -> DataFrame:
    """
    Within a single micro-batch the same business key can appear multiple
    times (e.g. a row updated 3 times in the last hour). We only want to
    apply the *latest* change per key to avoid the merge applying stale data
    or fighting itself. Keep the row with the max(order_col) per key.
    """
    window = Window.partitionBy(*primary_keys).orderBy(F.col(order_col).desc())
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def apply_cdc_merge(
    spark: SparkSession,
    changes_df: DataFrame,
    target_table_path: str,
    primary_keys: list,
    order_col: str = "source_ts",
) -> None:
    """
    Applies I/U/D changes into a Delta target using MERGE, keyed on
    primary_keys. This is the core upsert engine used by both silver and
    (optionally) gold dimension tables.

    - I / U  -> upsert (insert if new, update if exists)
    - D      -> soft delete (mark _is_deleted=true) so downstream consumers
                can distinguish "never existed" from "deleted", which is
                usually what BI / audit consumers need. Swap for a hard
                `.whenMatchedDelete()` if you truly want physical deletes.
    """
    changes_df = dedup_latest_change_per_key(changes_df, primary_keys, order_col)

    if not DeltaTable.isDeltaTable(spark, target_table_path):
        # First-ever load: just write inserts/updates, materialize deletes flag too.
        (
            changes_df.withColumn("_is_deleted", F.col("op_type") == F.lit("D"))
            .withColumn("_cdc_processed_at", F.current_timestamp())
            .write.format("delta")
            .mode("overwrite")
            .save(target_table_path)
        )
        return

    target = DeltaTable.forPath(spark, target_table_path)
    key_condition = " AND ".join([f"t.{k} = s.{k}" for k in primary_keys])

    staged = changes_df.withColumn(
        "_is_deleted", F.col("op_type") == F.lit("D")
    ).withColumn("_cdc_processed_at", F.current_timestamp())

    (
        target.alias("t")
        .merge(staged.alias("s"), key_condition)
        # Only apply the incoming change if it is actually newer than what's
        # already stored — this protects against out-of-order delivery
        # within the merge itself (belt-and-braces alongside the dedup above).
        .whenMatchedUpdateAll(condition=f"s.{order_col} >= t.{order_col}")
        .whenNotMatchedInsertAll()
        .execute()
    )
