"""
bigquery_utils.py
-----------------
Wraps the spark-bigquery-connector so gold (and optionally silver) jobs push
into BigQuery with consistent partitioning/clustering/write-mode behavior.

Requires the connector on the Spark cluster:
    --packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1
"""

from pyspark.sql import DataFrame


def write_df_to_bigquery(
    df: DataFrame,
    project_id: str,
    dataset: str,
    table: str,
    temp_gcs_bucket: str,
    write_mode: str = "append",
    partition_field: str | None = None,
    clustering_fields: list | None = None,
) -> None:
    """
    write_mode:
        'append'    -> incremental gold loads (most common for fact tables)
        'overwrite' -> full-refresh dimension tables
    partition_field: BigQuery time-partitioning column (e.g. order_date).
    clustering_fields: BigQuery clustering columns (e.g. ["customer_id"]).
    """
    writer = (
        df.write.format("bigquery")
        .option("table", f"{project_id}.{dataset}.{table}")
        .option("temporaryGcsBucket", temp_gcs_bucket)
        .option("createDisposition", "CREATE_IF_NEEDED")
        .mode(write_mode)
    )

    if partition_field:
        writer = writer.option("partitionField", partition_field)
    if clustering_fields:
        writer = writer.option("clusteredFields", ",".join(clustering_fields))

    writer.save()


def merge_into_bigquery(
    spark,
    df: DataFrame,
    project_id: str,
    dataset: str,
    table: str,
    primary_keys: list,
    temp_gcs_bucket: str,
    staging_dataset: str = None,
) -> None:
    """
    True upsert into BigQuery (the connector's DataFrame writer only supports
    append/overwrite, not MERGE). Pattern:
      1. Write the incremental batch to a staging table.
      2. Run a BigQuery MERGE statement via the BigQuery client to upsert
         staging -> target on primary_keys.
    Used for gold dimension tables that must reflect CDC updates/deletes,
    not just accumulate new rows.
    """
    from google.cloud import bigquery

    staging_dataset = staging_dataset or dataset
    staging_table = f"{table}_staging"

    write_df_to_bigquery(
        df,
        project_id,
        staging_dataset,
        staging_table,
        temp_gcs_bucket,
        write_mode="overwrite",
    )

    bq_client = bigquery.Client(project=project_id)
    key_cond = " AND ".join([f"T.{k} = S.{k}" for k in primary_keys])
    non_key_cols = [c for c in df.columns if c not in primary_keys]
    update_set = ", ".join([f"{c} = S.{c}" for c in non_key_cols])
    insert_cols = ", ".join(df.columns)
    insert_vals = ", ".join([f"S.{c}" for c in df.columns])

    merge_sql = f"""
    MERGE `{project_id}.{dataset}.{table}` T
    USING `{project_id}.{staging_dataset}.{staging_table}` S
    ON {key_cond}
    WHEN MATCHED AND S._is_deleted = TRUE THEN DELETE
    WHEN MATCHED THEN UPDATE SET {update_set}
    WHEN NOT MATCHED AND (S._is_deleted IS NULL OR S._is_deleted = FALSE)
        THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """
    bq_client.query(merge_sql).result()
