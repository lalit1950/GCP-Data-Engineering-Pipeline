"""
medallion_pipeline_dag.py
--------------------------
Airflow DAG orchestrating Bronze -> Silver -> Gold for each configured table.

Design notes:
- Each layer runs as a separate spark-submit job (SparkSubmitOperator), kept
  in their own files (bronze_ingestion.py / silver_transformation.py /
  gold_aggregation.py) so each layer can be tested, deployed, and scaled
  independently.
- Bronze -> Silver -> Gold is a strict dependency chain per table (silver
  needs bronze's output, gold needs silver's), enforced with `>>`.
- Tables are independent of each other and run in parallel via TaskGroups.
- retries + retry_delay give resilience against transient source/BQ issues.
- The DAG is scheduled hourly; watermark + grace-period logic (in
  watermark_utils.py) is what actually protects against late-arriving data,
  not the schedule itself.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup

TABLES = ["orders", "customers"]

SPARK_CONN_ID = "spark_default"
SPARK_PACKAGES = ",".join(
    [
        "io.delta:delta-core_2.12:2.4.0",
        "org.postgresql:postgresql:42.6.0",
        "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1",
    ]
)
PIPELINE_SRC_ZIP = "/opt/pipeline/src.zip"  # packaged src/ (config, common/*) shipped via --py-files

default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["data-eng-alerts@company.com"],
    "execution_timeout": timedelta(hours=2),
}


def _make_spark_task(task_id: str, script_relpath: str, table: str) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=task_id,
        conn_id=SPARK_CONN_ID,
        application=f"/opt/pipeline/src/{script_relpath}",
        py_files=PIPELINE_SRC_ZIP,
        packages=SPARK_PACKAGES,
        application_args=["--table", table, "--run_id", "{{ run_id }}"],
        conf={
            "spark.executor.memory": "4g",
            "spark.executor.cores": "2",
            "spark.dynamicAllocation.enabled": "true",
            "spark.sql.shuffle.partitions": "64",
        },
        verbose=False,
    )


def _check_quarantine_volume(table: str, **context):
    """
    Post-silver data-quality gate: fail loudly (and let Airflow alert /
    retry / page someone) if the quarantine volume for this run is
    abnormally high, instead of quietly letting bad data flow to gold.
    In production this would query the quarantine Delta/BQ table for
    row counts in this run_id and compare against a threshold.
    """
    import logging

    logging.info(f"[dq-gate] checking quarantine volume for {table}, run {context['run_id']}")
    # placeholder for real DQ threshold check against quarantine table
    return True


with DAG(
    dag_id="medallion_pipeline",
    description="Bronze -> Silver -> Gold pipeline with CDC + watermarking, landing in BigQuery",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["medallion", "cdc", "bigquery", "pyspark"],
) as dag:

    for table in TABLES:
        with TaskGroup(group_id=f"{table}_pipeline") as table_group:

            bronze_task = _make_spark_task(
                task_id=f"bronze_ingest_{table}",
                script_relpath="bronze/bronze_ingestion.py",
                table=table,
            )

            silver_task = _make_spark_task(
                task_id=f"silver_transform_{table}",
                script_relpath="silver/silver_transformation.py",
                table=table,
            )

            dq_gate_task = PythonOperator(
                task_id=f"dq_gate_{table}",
                python_callable=_check_quarantine_volume,
                op_kwargs={"table": table},
            )

            gold_task = _make_spark_task(
                task_id=f"gold_aggregate_{table}",
                script_relpath="gold/gold_aggregation.py",
                table=table,
            )

            bronze_task >> silver_task >> dq_gate_task >> gold_task
