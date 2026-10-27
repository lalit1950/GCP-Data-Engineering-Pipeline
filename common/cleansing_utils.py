"""
cleansing_utils.py
-------------------
Reusable data-quality / cleansing functions applied in the Silver layer.
Each function is small and composable so silver_transformation.py reads like
a pipeline of `.transform(...)` calls.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


def standardize_column_names(df: DataFrame) -> DataFrame:
    """snake_case, lower-case, strip whitespace from column names."""
    for c in df.columns:
        clean = c.strip().lower().replace(" ", "_")
        if clean != c:
            df = df.withColumnRenamed(c, clean)
    return df


def trim_string_columns(df: DataFrame) -> DataFrame:
    """Trim leading/trailing whitespace on every string column."""
    string_cols = [f.name for f in df.schema.fields if f.dataType.typeName() == "string"]
    for c in string_cols:
        df = df.withColumn(c, F.trim(F.col(c)))
    return df


def standardize_nulls(df: DataFrame, null_tokens=("", "NULL", "null", "N/A", "n/a", "NA")) -> DataFrame:
    """Convert common 'fake null' string tokens into real nulls."""
    string_cols = [f.name for f in df.schema.fields if f.dataType.typeName() == "string"]
    for c in string_cols:
        df = df.withColumn(c, F.when(F.col(c).isin(*null_tokens), None).otherwise(F.col(c)))
    return df


def enforce_schema(df: DataFrame, expected_schema: StructType) -> DataFrame:
    """
    Casts columns to the expected type and adds any missing expected column
    as null, so downstream Delta/BigQuery writes never fail on schema drift.
    Extra/unexpected columns coming from source are dropped here — log them
    upstream if you need drift alerts.
    """
    select_exprs = []
    for field in expected_schema.fields:
        if field.name in df.columns:
            select_exprs.append(F.col(field.name).cast(field.dataType).alias(field.name))
        else:
            select_exprs.append(F.lit(None).cast(field.dataType).alias(field.name))
    return df.select(*select_exprs)


def dedup_records(df: DataFrame, primary_keys: list, order_col: str) -> DataFrame:
    """Keep only the latest record per primary key (post-cleansing safety net)."""
    from pyspark.sql.window import Window

    window = Window.partitionBy(*primary_keys).orderBy(F.col(order_col).desc())
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def quarantine_invalid_records(
    df: DataFrame, rules: dict, quarantine_path: str, spark
) -> DataFrame:
    """
    Splits df into (valid_rows, invalid_rows) based on a dict of
    {rule_name: sql_condition_string_that_should_be_true_for_valid_rows}.
    Invalid rows are written to a quarantine Delta table for triage/alerting
    instead of silently dropped or (worse) silently let through.

    Example rules:
        {"pk_not_null": "order_id IS NOT NULL",
         "amount_non_negative": "order_amount >= 0",
         "valid_date": "order_date IS NOT NULL"}
    """
    combined_valid_condition = " AND ".join(rules.values())
    valid_df = df.filter(F.expr(combined_valid_condition))
    invalid_df = df.filter(~F.expr(combined_valid_condition))

    if invalid_df.take(1):
        failed_rule_exprs = [
            F.when(~F.expr(cond), F.lit(name)).otherwise(F.lit(None)) for name, cond in rules.items()
        ]
        invalid_annotated = invalid_df.withColumn(
            "_failed_rules",
            F.array_except(F.array(*failed_rule_exprs), F.array(F.lit(None).cast("string"))),
        ).withColumn("_quarantined_at", F.current_timestamp())

        (
            invalid_annotated.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(quarantine_path)
        )

    return valid_df


def add_audit_columns(df: DataFrame, pipeline_stage: str) -> DataFrame:
    """Adds standard lineage/audit columns used across bronze/silver/gold."""
    return (
        df.withColumn("_processed_at", F.current_timestamp())
        .withColumn("_pipeline_stage", F.lit(pipeline_stage))
    )
