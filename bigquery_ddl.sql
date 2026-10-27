-- BigQuery DDL for the Gold layer.
-- Note: write_df_to_bigquery() uses createDisposition=CREATE_IF_NEEDED so
-- these tables will also auto-create on first write, but explicit DDL is
-- kept here for review/change-control and to lock in partitioning/clustering
-- choices deliberately rather than relying on connector defaults.

CREATE SCHEMA IF NOT EXISTS `my-gcp-project.analytics_gold`;

CREATE TABLE IF NOT EXISTS `my-gcp-project.analytics_gold.fact_daily_orders` (
  order_date        DATE,
  order_status      STRING,
  order_count       INT64,
  total_amount      FLOAT64,
  avg_order_amount  FLOAT64,
  _processed_at     TIMESTAMP,
  _pipeline_stage   STRING
)
PARTITION BY order_date
CLUSTER BY order_status;

CREATE TABLE IF NOT EXISTS `my-gcp-project.analytics_gold.dim_customers` (
  customer_id    STRING,
  customer_name  STRING,
  email          STRING,
  country        STRING,
  updated_at     TIMESTAMP,
  _is_deleted    BOOL,
  _processed_at  TIMESTAMP,
  _pipeline_stage STRING
)
CLUSTER BY customer_id;

-- Staging tables used by bigquery_utils.merge_into_bigquery() are created
-- automatically per run (table_staging) with createDisposition=CREATE_IF_NEEDED.
