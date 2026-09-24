"""Airflow DAG: daily incremental run of the support-signals lakehouse.

ingest -> silver -> dq_silver -> gold -> dq_gold -> [ml, load_warehouse]

* SIGNALS_RUN_MODE=local  every task shells out to `python -m signals <stage>` (docker-compose dev)
* SIGNALS_RUN_MODE=aws    Spark stages run as AWS Glue jobs (created by Terraform);
                          ingest/load run in the Airflow worker (MWAA) with boto3.

Reliability settings: retries with exponential backoff, a per-task SLA,
max_active_runs=1 (runs never overlap, so watermarks can't race), and a
`full_refresh` param to rebuild silver/gold from all bronze (the backfill path).
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

MODE = os.environ.get("SIGNALS_RUN_MODE", "local")
REPO = os.environ.get("SIGNALS_REPO_DIR", "/opt/airflow/project")
RUN_ID = "{{ ts_nodash }}"
FULL = "{{ '--full-refresh' if params.full_refresh else '' }}"
CW = "--cloudwatch" if MODE == "aws" else ""

default_args = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=1),
    "sla": timedelta(hours=2),
}


def cli(task_id: str, args: str) -> BashOperator:
    return BashOperator(task_id=task_id, cwd=REPO,
                        bash_command=f"python -m signals {args} --run-id {RUN_ID}",
                        env={**os.environ, "PYTHONPATH": f"{REPO}/src"}, append_env=True)


def spark_stage(task_id: str, args: str):
    if MODE != "aws":
        return cli(task_id, args)
    from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

    return GlueJobOperator(
        task_id=task_id, job_name=os.environ.get("SIGNALS_GLUE_JOB", "support-signals-spark"),
        script_args={"--stage_args": f"{args} --run-id {RUN_ID}"}, wait_for_completion=True,
        verbose=True)


with DAG(
    dag_id="support_signals_lakehouse",
    description="GitHub AWS issues -> S3 bronze/silver/gold -> warehouse, with DQ gates",
    schedule="0 6 * * *",            # daily 06:00 UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,                   # ingestion is watermark-driven, not interval-driven
    max_active_runs=1,
    default_args=default_args,
    params={"full_refresh": Param(False, type="boolean", description="rebuild silver/gold from all bronze")},
    tags=["lakehouse", "support", "aws"],
) as dag:
    ingest = cli("ingest_github", "ingest")
    to_silver = spark_stage("bronze_to_silver", f"silver {FULL}")
    dq_silver = spark_stage("dq_silver", f"dq --layer silver {CW}")
    to_gold = spark_stage("silver_to_gold", f"gold {FULL}")
    dq_gold = spark_stage("dq_gold", f"dq --layer gold {CW}")
    ml = spark_stage("ml_topics_and_risk", "ml")
    load = cli("load_warehouse", f"load --target {'redshift' if MODE == 'aws' else 'duckdb'}")

    ingest >> to_silver >> dq_silver >> to_gold >> dq_gold >> [ml, load]
