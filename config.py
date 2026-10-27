"""
config.py
---------
Central configuration for the medallion pipeline.
In production, swap the hard-coded dict for Airflow Variables / GCP Secret Manager /
a config service, but keep the SAME keys so downstream code doesn't change.
"""

import os

# ---------------------------------------------------------------------------
# GCP / BigQuery
# ---------------------------------------------------------------------------
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "my-gcp-project")
BQ_DATASET_SILVER = "analytics_silver"
BQ_DATASET_GOLD = "analytics_gold"
# GCS bucket the Spark <-> BigQuery connector uses for staging (required by connector)
BQ_TEMP_GCS_BUCKET = os.environ.get("BQ_TEMP_GCS_BUCKET", "my-project-bq-staging")

# ---------------------------------------------------------------------------
# Storage layer paths (GCS-backed Delta Lake). Delta gives us ACID MERGE,
# which is what makes CDC upserts and idempotent re-runs possible.
# ---------------------------------------------------------------------------
GCS_ROOT = os.environ.get("GCS_ROOT", "gs://my-project-datalake")
BRONZE_PATH = f"{GCS_ROOT}/bronze"
SILVER_PATH = f"{GCS_ROOT}/silver"
GOLD_PATH = f"{GCS_ROOT}/gold"
QUARANTINE_PATH = f"{GCS_ROOT}/quarantine"
CONTROL_TABLE_PATH = f"{GCS_ROOT}/_control/watermarks"

# ---------------------------------------------------------------------------
# Source system (JDBC). Assumes the source exposes CDC metadata columns:
#   op_type       -> 'I' | 'U' | 'D'
#   source_ts     -> event/commit timestamp from the source (used for watermarking)
# This matches what tools like Debezium / DMS / native CDC logs typically emit.
# If your source has no native CDC feed, see cdc_utils.derive_cdc_from_snapshot()
# for a hash-diff fallback.
# ---------------------------------------------------------------------------
JDBC_URL = os.environ.get("SOURCE_JDBC_URL", "jdbc:postgresql://source-db:5432/orders")
JDBC_PROPERTIES = {
    "user": os.environ.get("SOURCE_DB_USER", "readonly_user"),
    "password": os.environ.get("SOURCE_DB_PASSWORD", ""),
    "driver": "org.postgresql.Driver",
}

# ---------------------------------------------------------------------------
# Per-table pipeline configuration.
# primary_keys      -> business key used for CDC merge / dedup
# event_time_col    -> column used for watermarking (late-arrival detection)
# late_arrival_grace-> how far back we re-open the window to catch stragglers
# ---------------------------------------------------------------------------
TABLE_CONFIG = {
    "orders": {
        "source_table": "public.orders",
        "primary_keys": ["order_id"],
        "event_time_col": "updated_at",
        "late_arrival_grace_minutes": 60,      # re-scan last 60 min every run
        "max_lookback_hours": 72,               # hard ceiling for backfills/outage recovery
        "gold_partition_field": "order_date",
    },
    "customers": {
        "source_table": "public.customers",
        "primary_keys": ["customer_id"],
        "event_time_col": "updated_at",
        "late_arrival_grace_minutes": 30,
        "max_lookback_hours": 48,
        "gold_partition_field": None,
    },
}

SPARK_APP_NAME = "medallion-pipeline"
SHUFFLE_PARTITIONS = int(os.environ.get("SPARK_SHUFFLE_PARTITIONS", "64"))
