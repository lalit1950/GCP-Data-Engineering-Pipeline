"""
silver_transformation.py
-------------------------
Silver layer: reads new bronze rows since the last silver watermark, cleanses
them, deduplicates, quarantines bad records, and applies a CDC MERGE into the
silver Delta table so silver always reflects the latest, clean state per
business key (unlike bronze which is an append-only log).

Usage:
    spark-submit silver_transformation.py --table orders --run_id <airflow_run_id>
"""

import argparse
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType

sys.path.append("/opt/pipeline/src")
from config import config
from common import watermark_utils, cdc_utils, cleansing_utils


# Expected silver schema per table — enforced so BigQuery/gold never sees
# schema drift from an upstream source change slipping through unnoticed.
SILVER_SCHEMAS = {
    "orders": StructType(
        [
            StructField("order_id", StringType()),
            StructField("customer_id", StringType()),
            StructField("order_amount", DoubleType()),
            StructField("order_status", StringType()),
            StructField("order_date", TimestampType()),
            StructField("updated_at", TimestampType()),
            StructField("op_type", StringType()),
            StructField("source_ts", TimestampType()),
        ]
    ),
    "customers": StructType(
        [
            StructField("customer_id", StringType()),
            StructField("customer_name", StringType()),
            StructField("email", StringType()),
            StructField("country", StringType()),
            StructField("updated_at", TimestampType()),
            StructField("op_type", StringType()),
            StructField("source_ts", TimestampType()),
        ]
    ),
}

DQ_RULES = {
    "orders": {
        "pk_not_null": "order_id IS NOT NULL",
        "amount_non_negative": "order_amount IS NULL OR order_amount >= 0",
        "valid_status": "order_status IN ('PLACED','SHIPPED','DELIVERED','CANCELLED','RETURNED')",
    },
    "customers": {
        "pk_not_null": "customer_id IS NOT NULL",
        "valid_email": "email IS NULL OR email RLIKE '^[^@]+@[^@]+\\\\.[^@]+$'",
    },
}


def run(table_name: str, run_id: str) -> None:
    cfg = config.TABLE_CONFIG[table_name]

    spark = (
        SparkSession.builder.appName(f"{config.SPARK_APP_NAME}-silver-{table_name}")
        .config("spark.sql.shuffle.partitions", config.SHUFFLE_PARTITIONS)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    bronze_table_path = f"{config.BRONZE_PATH}/{table_name}"
    silver_table_path = f"{config.SILVER_PATH}/{table_name}"
    quarantine_path = f"{config.QUARANTINE_PATH}/{table_name}"

    # 1. Watermark-bounded read from BRONZE (not the source) — silver only
    #    ever reads from bronze, keeping bronze as the single extraction point.
    window_start, window_end = watermark_utils.compute_read_window(
        spark,
        config.CONTROL_TABLE_PATH,
        table_name=f"silver_{table_name}",
        late_arrival_grace_minutes=cfg["late_arrival_grace_minutes"],
        max_lookback_hours=cfg["max_lookback_hours"],
    )
    print(f"[silver:{table_name}] reading bronze window {window_start} -> {window_end}")

    bronze_df = (
        spark.read.format("delta")
        .load(bronze_table_path)
        .filter(
            (F.col(cfg["event_time_col"]) >= F.lit(window_start))
            & (F.col(cfg["event_time_col"]) < F.lit(window_end))
        )
    )

    if bronze_df.rdd.isEmpty():
        print(f"[silver:{table_name}] no new bronze rows in window, exiting cleanly")
        spark.stop()
        return

    # 2. Cleansing pipeline (chain of small, testable transforms).
    cleansed_df = (
        bronze_df.transform(cleansing_utils.standardize_column_names)
        .transform(cleansing_utils.trim_string_columns)
        .transform(cleansing_utils.standardize_nulls)
        .transform(lambda df: cleansing_utils.enforce_schema(df, SILVER_SCHEMAS[table_name]))
    )

    # 3. Quarantine records that fail data-quality rules instead of dropping
    #    or silently propagating them.
    valid_df = cleansing_utils.quarantine_invalid_records(
        cleansed_df, DQ_RULES[table_name], quarantine_path, spark
    )

    # 4. Dedup multiple changes to the same key within this batch (keep latest),
    #    then apply as a CDC MERGE (upsert/soft-delete) into the silver table.
    #    This merge is what makes late-arriving AND out-of-order records safe:
    #    it's keyed on business PK and only accepts newer source_ts.
    cdc_utils.apply_cdc_merge(
        spark,
        changes_df=valid_df,
        target_table_path=silver_table_path,
        primary_keys=cfg["primary_keys"],
        order_col="source_ts",
    )

    # 5. Advance silver watermark to the max event time actually processed.
    max_event_time = valid_df.agg(F.max(cfg["event_time_col"])).collect()[0][0]
    watermark_utils.update_watermark(
        spark, config.CONTROL_TABLE_PATH, f"silver_{table_name}", max_event_time
    )

    print(f"[silver:{table_name}] merged {valid_df.count()} rows, watermark -> {max_event_time}")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True, choices=list(config.TABLE_CONFIG.keys()))
    parser.add_argument("--run_id", required=True)
    args = parser.parse_args()
    run(args.table, args.run_id)
