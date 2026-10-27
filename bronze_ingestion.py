"""
bronze_ingestion.py
--------------------
Bronze (raw) layer: extracts incremental CDC changes from the source system
and lands them AS-IS (append-only, minimally touched) into Delta, tagged
with ingestion metadata. Bronze is the immutable audit log — never mutate
or dedup here; that happens in Silver.

Usage:
    spark-submit bronze_ingestion.py --table orders --run_id <airflow_run_id>
"""

import argparse
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.append("/opt/pipeline/src")  # see dags/medallion_pipeline_dag.py for packaging
from config import config
from common import watermark_utils, cdc_utils


def run(table_name: str, run_id: str) -> None:
    cfg = config.TABLE_CONFIG[table_name]

    spark = (
        SparkSession.builder.appName(f"{config.SPARK_APP_NAME}-bronze-{table_name}")
        .config("spark.sql.shuffle.partitions", config.SHUFFLE_PARTITIONS)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    bronze_table_path = f"{config.BRONZE_PATH}/{table_name}"

    # 1. Determine the read window using the watermark (handles late data:
    #    we re-open a trailing grace window instead of reading only "new" rows).
    window_start, window_end = watermark_utils.compute_read_window(
        spark,
        config.CONTROL_TABLE_PATH,
        table_name=f"bronze_{table_name}",
        late_arrival_grace_minutes=cfg["late_arrival_grace_minutes"],
        max_lookback_hours=cfg["max_lookback_hours"],
    )
    print(f"[bronze:{table_name}] reading window {window_start} -> {window_end}")

    # 2. Extract incremental CDC rows from the source (native CDC feed assumed;
    #    see cdc_utils.derive_cdc_from_snapshot for the snapshot-diff fallback).
    raw_df = cdc_utils.read_incremental_jdbc(
        spark,
        jdbc_url=config.JDBC_URL,
        jdbc_properties=config.JDBC_PROPERTIES,
        source_table=cfg["source_table"],
        event_time_col=cfg["event_time_col"],
        window_start=window_start,
        window_end=window_end,
    )

    if raw_df.rdd.isEmpty():
        print(f"[bronze:{table_name}] no new/changed rows in window, exiting cleanly")
        spark.stop()
        return

    # 3. Tag with ingestion metadata. Bronze keeps every version of a row
    #    (append-only) so it doubles as an audit trail / replay source.
    tagged_df = (
        raw_df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_system", F.lit("postgres_orders_db"))
        .withColumn("_airflow_run_id", F.lit(run_id))
        .withColumn("_batch_window_start", F.lit(window_start))
        .withColumn("_batch_window_end", F.lit(window_end))
    )

    (
        tagged_df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("_source_system")
        .save(bronze_table_path)
    )

    # 4. Advance the bronze watermark to the max event time actually seen,
    #    NOT to window_end — this is what makes late-arrival re-scans correct
    #    (we only move the watermark as far as data we've actually confirmed).
    max_event_time = raw_df.agg(F.max(cfg["event_time_col"])).collect()[0][0]
    watermark_utils.update_watermark(
        spark, config.CONTROL_TABLE_PATH, f"bronze_{table_name}", max_event_time
    )

    print(f"[bronze:{table_name}] wrote {tagged_df.count()} rows, watermark -> {max_event_time}")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True, choices=list(config.TABLE_CONFIG.keys()))
    parser.add_argument("--run_id", required=True)
    args = parser.parse_args()
    run(args.table, args.run_id)
