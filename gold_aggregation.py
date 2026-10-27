"""
gold_aggregation.py
---------------------
Gold layer: reads the clean, deduped Silver Delta table, applies business
aggregation / dimensional modeling, and pushes the result into BigQuery.

Two gold patterns are shown:
  - orders    -> fact table, daily aggregation, appended incrementally to BQ,
                 partitioned by order_date.
  - customers -> dimension table, full CDC-aware upsert into BQ via MERGE
                 (so BQ reflects updates/deletes from silver, not just inserts).

Usage:
    spark-submit gold_aggregation.py --table orders --run_id <airflow_run_id>
"""

import argparse
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.append("/opt/pipeline/src")
from config import config
from common import watermark_utils, bigquery_utils, cleansing_utils


def build_orders_gold(spark, silver_df):
    """Daily order fact aggregation."""
    return (
        silver_df.filter(F.col("_is_deleted") == False)  # noqa: E712
        .withColumn("order_date_day", F.to_date("order_date"))
        .groupBy("order_date_day", "order_status")
        .agg(
            F.count("*").alias("order_count"),
            F.sum("order_amount").alias("total_amount"),
            F.avg("order_amount").alias("avg_order_amount"),
        )
        .withColumnRenamed("order_date_day", "order_date")
    )


def build_customers_gold(spark, silver_df):
    """Customer dimension, CDC-aware (includes _is_deleted for MERGE)."""
    return silver_df.select(
        "customer_id", "customer_name", "email", "country", "updated_at", "_is_deleted"
    )


GOLD_BUILDERS = {
    "orders": build_orders_gold,
    "customers": build_customers_gold,
}


def run(table_name: str, run_id: str) -> None:
    cfg = config.TABLE_CONFIG[table_name]

    spark = (
        SparkSession.builder.appName(f"{config.SPARK_APP_NAME}-gold-{table_name}")
        .config("spark.sql.shuffle.partitions", config.SHUFFLE_PARTITIONS)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    silver_table_path = f"{config.SILVER_PATH}/{table_name}"

    window_start, window_end = watermark_utils.compute_read_window(
        spark,
        config.CONTROL_TABLE_PATH,
        table_name=f"gold_{table_name}",
        late_arrival_grace_minutes=cfg["late_arrival_grace_minutes"],
        max_lookback_hours=cfg["max_lookback_hours"],
    )
    print(f"[gold:{table_name}] reading silver window {window_start} -> {window_end}")

    silver_df = (
        spark.read.format("delta")
        .load(silver_table_path)
        .filter(
            (F.col(cfg["event_time_col"]) >= F.lit(window_start))
            & (F.col(cfg["event_time_col"]) < F.lit(window_end))
        )
    )

    if silver_df.rdd.isEmpty():
        print(f"[gold:{table_name}] no new silver rows in window, exiting cleanly")
        spark.stop()
        return

    gold_df = GOLD_BUILDERS[table_name](spark, silver_df).transform(
        lambda df: cleansing_utils.add_audit_columns(df, "gold")
    )

    if table_name == "customers":
        # Dimension table -> upsert/delete semantics via BigQuery MERGE.
        bigquery_utils.merge_into_bigquery(
            spark,
            gold_df,
            project_id=config.GCP_PROJECT_ID,
            dataset=config.BQ_DATASET_GOLD,
            table="dim_customers",
            primary_keys=cfg["primary_keys"],
            temp_gcs_bucket=config.BQ_TEMP_GCS_BUCKET,
        )
    else:
        # Fact table -> simple partitioned append (idempotency handled by
        # re-running the same watermark window being a harmless re-append;
        # if exactly-once is required, swap to merge_into_bigquery keyed on
        # (order_date, order_status) instead).
        bigquery_utils.write_df_to_bigquery(
            gold_df,
            project_id=config.GCP_PROJECT_ID,
            dataset=config.BQ_DATASET_GOLD,
            table="fact_daily_orders",
            temp_gcs_bucket=config.BQ_TEMP_GCS_BUCKET,
            write_mode="append",
            partition_field=cfg["gold_partition_field"],
            clustering_fields=["order_status"],
        )

    max_event_time = silver_df.agg(F.max(cfg["event_time_col"])).collect()[0][0]
    watermark_utils.update_watermark(
        spark, config.CONTROL_TABLE_PATH, f"gold_{table_name}", max_event_time
    )

    print(f"[gold:{table_name}] pushed {gold_df.count()} rows to BigQuery, watermark -> {max_event_time}")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True, choices=list(config.TABLE_CONFIG.keys()))
    parser.add_argument("--run_id", required=True)
    args = parser.parse_args()
    run(args.table, args.run_id)
