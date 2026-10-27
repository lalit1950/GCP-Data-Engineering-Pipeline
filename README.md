# Medallion Data Pipeline (Airflow + PySpark + Delta Lake + BigQuery)

End-to-end batch pipeline implementing Bronze/Silver/Gold with CDC ingestion,
watermark-based late-data handling, data cleansing, and a BigQuery sink.

## Project layout

```
data-pipeline/
├── dags/
│   └── medallion_pipeline_dag.py     # Airflow orchestration (per-table TaskGroups)
├── src/
│   ├── bronze/bronze_ingestion.py    # Raw CDC extract -> Delta (append-only)
│   ├── silver/silver_transformation.py  # Cleansing + CDC merge -> Delta
│   ├── gold/gold_aggregation.py      # Aggregation + push to BigQuery
│   ├── common/
│   │   ├── cdc_utils.py              # CDC read + MERGE upsert/delete logic
│   │   ├── watermark_utils.py        # Watermark control table + late-arrival window
│   │   ├── cleansing_utils.py        # Cleansing/DQ/quarantine functions
│   │   └── bigquery_utils.py         # BQ append + BQ MERGE upsert helpers
│   └── config/config.py              # All paths, per-table settings, JDBC config
├── sql/bigquery_ddl.sql              # Gold table DDL (partitioning/clustering)
└── requirements.txt
```

## Architecture

```
Source DB (CDC feed: op_type, source_ts)
        │  incremental JDBC read, watermark-bounded window
        ▼
  BRONZE (Delta, append-only, raw + ingestion metadata)
        │  cleansing, DQ rules, quarantine, dedup-latest-per-key, CDC MERGE
        ▼
  SILVER (Delta, upserted, one row per business key, clean + typed)
        │  business aggregation / dimensional modeling
        ▼
  GOLD (Delta, optional) ──push──► BigQuery (fact: append+partitioned,
                                    dimension: MERGE upsert/delete)
```

## CDC strategy

Two supported modes (`src/common/cdc_utils.py`):

1. **Native CDC** (default/preferred): source exposes `op_type` (`I`/`U`/`D`)
   and `source_ts` per row — the pattern produced by Debezium, AWS DMS, or a
   database's native change-log/CDC feature. Bronze does an incremental JDBC
   pull filtered on the event-time column.
2. **Snapshot-diff fallback** (`derive_cdc_from_snapshot`): if the source has
   no CDC feed, hash all non-key columns of the current vs. previous
   snapshot to synthesize `I`/`U`/`D`.

Both converge on `apply_cdc_merge()`, which:
- dedups multiple changes to the same key within a batch (keeps the latest
  by `source_ts`),
- MERGEs into the Delta target keyed on the business primary key,
- only applies an incoming change if its `source_ts` is >= the target's
  (protects against out-of-order delivery),
- marks deletes as `_is_deleted = true` (soft delete) rather than physically
  removing rows, so downstream BI/audits can distinguish "deleted" from
  "never existed." Swap for `.whenMatchedDelete()` if hard deletes are required.

## Watermark / late-arriving data logic

`src/common/watermark_utils.py` maintains a Delta **control table**
(`table_name | last_watermark | last_run_ts`), one row per pipeline stage
per table (`bronze_orders`, `silver_orders`, `gold_orders`, ...).

Each run computes a read window as:

```
window_start = max(last_watermark - late_arrival_grace_minutes,
                    now - max_lookback_hours)
window_end   = now
```

- Subtracting a **grace period** re-opens a trailing window every run, so a
  record whose event actually happened earlier but arrived late (clock skew,
  offline mobile sync, retried source transaction, etc.) still gets swept up
  on a subsequent run instead of being permanently missed.
- `max_lookback_hours` caps how far back a run will ever scan, so a
  multi-day outage doesn't trigger an unbounded full-table scan on recovery.
- The watermark is only advanced to the **max event time actually observed**
  in the batch (not to `window_end`), and only moves forward
  (`whenMatchedUpdate(condition="s.last_watermark > t.last_watermark")`).
- Because every write from bronze onward is either append-only (bronze) or
  a keyed MERGE (silver/gold/BigQuery), re-reading the same overlapping
  window on every run is safe and idempotent — re-processing an unchanged
  record is a no-op merge.

## Data cleansing (Silver layer)

`src/common/cleansing_utils.py`, chained via `.transform(...)`:
1. `standardize_column_names` — snake_case, trims whitespace in column names.
2. `trim_string_columns` — trims value whitespace on all string columns.
3. `standardize_nulls` — normalizes `"", "NULL", "N/A"` etc. into real nulls.
4. `enforce_schema` — casts to the expected typed schema; missing columns
   become null instead of failing the job (schema-drift tolerant).
5. `quarantine_invalid_records` — evaluates named DQ rules (e.g.
   `amount_non_negative`, `valid_email`); failing rows are written to a
   separate quarantine Delta table annotated with which rule(s) failed,
   rather than being silently dropped.
6. `dedup_records` / CDC dedup — keeps only the latest row per business key.

## BigQuery sink

`src/common/bigquery_utils.py`:
- `write_df_to_bigquery` — partitioned/clustered append or full overwrite,
  used for fact tables (`fact_daily_orders`).
- `merge_into_bigquery` — writes the incremental batch to a `_staging` table,
  then runs a generated `MERGE` statement via the BigQuery client to upsert
  and soft-delete into the target — used for CDC-aware dimension tables
  (`dim_customers`), since the Spark BigQuery connector's DataFrame writer
  only supports append/overwrite, not native upsert.

## Airflow DAG

`dags/medallion_pipeline_dag.py`:
- One `TaskGroup` per table (`orders_pipeline`, `customers_pipeline`),
  running in parallel.
- Inside each group: `bronze_ingest >> silver_transform >> dq_gate >>
  gold_aggregate`, a strict chain since each layer depends on the previous
  layer's output.
- Each Spark step runs via `SparkSubmitOperator` against its own file
  (bronze/silver/gold kept separate per your requirement, so they can be
  versioned, tested, and scaled independently).
- `dq_gate` is a `PythonOperator` checkpoint that can hard-fail the DAG (and
  page the on-call) if quarantine volume for a run is abnormal, before bad
  data reaches Gold/BigQuery.
- Hourly schedule; retries=3 with 5-minute backoff for resilience against
  transient source/BigQuery issues; `max_active_runs=1` to avoid overlapping
  watermark updates.

## Running locally

```bash
pip install -r requirements.txt

# Package src/ so Spark can import config/ and common/ on the cluster
cd src && zip -r ../src.zip config common && cd ..

spark-submit \
  --packages io.delta:delta-core_2.12:2.4.0,org.postgresql:postgresql:42.6.0,com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1 \
  --py-files src.zip \
  src/bronze/bronze_ingestion.py --table orders --run_id manual_test_1

spark-submit ... src/silver/silver_transformation.py --table orders --run_id manual_test_1
spark-submit ... src/gold/gold_aggregation.py --table orders --run_id manual_test_1
```

Deploy `dags/medallion_pipeline_dag.py` to your Airflow `dags/` folder, and
ship `src/` (as `src.zip`) plus the DAG's expected path `/opt/pipeline/src`
to your Spark cluster / workers.

## Extending to a new table

1. Add an entry to `TABLE_CONFIG` in `src/config/config.py` (source table,
   primary keys, event-time column, grace period, lookback ceiling).
2. Add its expected schema + DQ rules in `silver_transformation.py`.
3. Add a gold builder function in `gold_aggregation.py`.
4. Add the table name to `TABLES` in the DAG — a new parallel TaskGroup is
   created automatically.
