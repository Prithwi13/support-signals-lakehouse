"""SparkSession factory + helpers for versioned table snapshots.

Tables are written as immutable snapshots: <table>/v=<run_id>/part-*.parquet plus a
tiny pointer file <table>/_current.json. Readers always resolve the pointer.
This gives atomic publish (swap one small file), safe re-runs and one-step
rollback, on both local disk and S3, without needing a table format.
(On AWS the natural upgrade is Apache Iceberg with MERGE INTO; see README.)
"""
from __future__ import annotations

import os
import uuid

from pyspark.sql import DataFrame, SparkSession

from signals import storage
from signals.snapshots import current_version
from signals.snapshots import pointer_uri as _pointer


def get_spark(app: str = "support-signals") -> SparkSession:
    active = SparkSession.getActiveSession()
    if active:
        return active
    b = (SparkSession.builder.appName(app)
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"))
         .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
         .config("spark.ui.showConsoleProgress", "false"))
    if not os.environ.get("GLUE_JOB"):  # Glue supplies its own master/config
        b = (b.master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "3g")))
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_table(spark: SparkSession, table_uri: str) -> DataFrame | None:
    v = current_version(table_uri)
    if v is None:
        return None
    return spark.read.parquet(f"{table_uri.rstrip('/')}/v={v}")


def publish_table(df: DataFrame, table_uri: str, version: str, partition_by: list[str] | None = None,
                  coalesce: int | None = None) -> int:
    """Write a new snapshot, then flip the pointer. Returns the row count written.

    Each attempt gets its own directory (run id + short suffix). That matters on retries:
    an Airflow retry of the same run reads the snapshot it previously published and must
    never overwrite the directory it is reading from.
    """
    version = f"{version}.{uuid.uuid4().hex[:6]}"
    target = f"{table_uri.rstrip('/')}/v={version}"
    out = df.coalesce(coalesce) if coalesce else df
    w = out.write.mode("overwrite")
    if partition_by:
        w = w.partitionBy(*partition_by)
    w.parquet(target)
    n = df.sparkSession.read.parquet(target).count()
    storage.write_json(_pointer(table_uri), {"version": version, "rows": n})
    return n
