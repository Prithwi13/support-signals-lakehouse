"""Bronze (raw JSON) -> Silver (clean, typed, deduplicated, current-state tables).

What this layer guarantees
--------------------------
* Typed columns from an explicit contract schema (no schema inference on raw data).
* Schema drift detection: new/unknown top-level fields in the payload are counted
  and reported instead of silently dropped or breaking the job.
* Pull requests (which GitHub's issues API also returns) are filtered out.
* Deduplication + late-arriving updates: one row per issue, and the version with
  the greatest (updated_at, _ingested_at) always wins, even if it arrives in an
  older bronze file or is re-read by the ingestion lookback window.
* Incremental: only bronze partitions newer than the last processed ingest_date
  are read, then MERGED with the previous silver snapshot.
"""
from __future__ import annotations

import logging
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from signals import storage
from signals.config import Config
from signals.transform.spark import publish_table, read_table

log = logging.getLogger(__name__)

ENVELOPE = T.StructType([
    T.StructField("_run_id", T.StringType()),
    T.StructField("_ingested_at", T.StringType()),
    T.StructField("_source", T.StringType()),
    T.StructField("repo", T.StringType()),
    T.StructField("payload", T.StringType()),  # nested object kept as raw JSON text
])

_USER = T.StructType([T.StructField("login", T.StringType()), T.StructField("type", T.StringType())])

ISSUE_CONTRACT = T.StructType([
    T.StructField("id", T.LongType()),
    T.StructField("number", T.LongType()),
    T.StructField("title", T.StringType()),
    T.StructField("body", T.StringType()),
    T.StructField("state", T.StringType()),
    T.StructField("state_reason", T.StringType()),
    T.StructField("user", _USER),
    T.StructField("author_association", T.StringType()),
    T.StructField("labels", T.ArrayType(T.StructType([T.StructField("name", T.StringType())]))),
    T.StructField("assignees", T.ArrayType(_USER)),
    T.StructField("milestone", T.StructType([T.StructField("title", T.StringType())])),
    T.StructField("comments", T.LongType()),
    T.StructField("reactions", T.StructType([T.StructField("total_count", T.LongType())])),
    T.StructField("pull_request", T.StructType([T.StructField("url", T.StringType())])),
    T.StructField("html_url", T.StringType()),
    T.StructField("created_at", T.StringType()),
    T.StructField("updated_at", T.StringType()),
    T.StructField("closed_at", T.StringType()),
])

COMMENT_CONTRACT = T.StructType([
    T.StructField("id", T.LongType()),
    T.StructField("issue_url", T.StringType()),
    T.StructField("body", T.StringType()),
    T.StructField("user", _USER),
    T.StructField("author_association", T.StringType()),
    T.StructField("created_at", T.StringType()),
    T.StructField("updated_at", T.StringType()),
])

# Fields GitHub sends that we deliberately don't model. Anything NOT here and NOT
# in the contract is reported as drift.
KNOWN_IGNORED = {
    "issues": {"url", "repository_url", "labels_url", "comments_url", "events_url", "node_id", "assignee",
               "locked", "active_lock_reason", "closed_by", "performed_via_github_app", "timeline_url",
               "draft", "type", "sub_issues_summary", "issue_dependencies_summary", "body_html",
               "body_text", "pinned_comment", "parent_issue_url", "issue_field_values"},
    # "pin" and "minimized" were flagged by the drift check on the first real run (Sep 2026)
    # and reviewed: moderation metadata we don't need, so they're acknowledged here.
    "issue_comments": {"url", "html_url", "node_id", "performed_via_github_app", "reactions", "pin", "minimized"},
}
MAINTAINER_ASSOC = ["OWNER", "MEMBER", "COLLABORATOR"]


def _ts(c: str) -> F.Column:
    return F.to_timestamp(F.col(c), "yyyy-MM-dd'T'HH:mm:ss'Z'")


def read_bronze(spark: SparkSession, cfg: Config, entity: str, min_ingest_date: str | None) -> DataFrame | None:
    base = f"{cfg.bronze}/{entity}"
    try:
        raw = spark.read.option("basePath", base).text(f"{base}/repo=*/ingest_date=*/*.jsonl.gz")
    except Exception as e:  # AnalysisException when no files yet
        if "PATH_NOT_FOUND" in str(e) or "Path does not exist" in str(e):
            return None
        raise
    if min_ingest_date:
        raw = raw.where(F.col("ingest_date") >= F.lit(min_ingest_date))
    return raw.select(F.from_json("value", ENVELOPE).alias("e"), "ingest_date").select("e.*", "ingest_date")


def schema_drift(env: DataFrame, contract: T.StructType, entity: str) -> dict[str, int]:
    """Count records carrying top-level keys that are neither modeled nor known-ignored."""
    known = {f.name for f in contract.fields} | KNOWN_IGNORED[entity]
    keys = (env.select(F.explode(F.json_object_keys("payload")).alias("k"))
            .where(~F.col("k").isin(list(known))).groupBy("k").count().collect())
    return {r["k"]: r["count"] for r in keys}


def _latest(df: DataFrame, key: str) -> DataFrame:
    w = Window.partitionBy(key).orderBy(F.col("updated_at").desc(), F.col("_ingested_at").desc())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def build_issues(env: DataFrame) -> DataFrame:
    p = env.select("repo", "_run_id", _ts("_ingested_at").alias("_ingested_at"),
                   F.from_json("payload", ISSUE_CONTRACT).alias("p"))
    labels = F.transform(F.col("p.labels"), lambda x: F.lower(x["name"]))
    return (p.where(F.col("p.pull_request").isNull())         # drop PRs
            .where(F.col("p.id").isNotNull())                  # malformed payloads
            .select(
                F.col("p.id").alias("issue_id"),
                F.concat_ws("#", "repo", F.col("p.number").cast("string")).alias("issue_key"),
                "repo",
                F.col("p.number").alias("number"),
                F.col("p.title").alias("title"),
                F.lower("p.state").alias("state"),
                F.col("p.state_reason").alias("state_reason"),
                F.col("p.user.login").alias("author_login"),
                F.col("p.user.type").alias("author_type"),
                F.col("p.author_association").alias("author_association"),
                F.coalesce(labels, F.array().cast("array<string>")).alias("labels"),
                F.coalesce(F.size("p.assignees"), F.lit(0)).alias("assignee_count"),
                F.col("p.milestone.title").alias("milestone"),
                F.coalesce("p.comments", F.lit(0)).alias("comment_count"),
                F.coalesce("p.reactions.total_count", F.lit(0)).alias("reaction_count"),
                F.coalesce(F.length("p.body"), F.lit(0)).alias("body_len"),
                F.coalesce(F.col("p.body").contains("```"), F.lit(False)).alias("has_code_block"),
                F.col("p.html_url").alias("html_url"),
                _ts("p.created_at").alias("created_at"),
                _ts("p.updated_at").alias("updated_at"),
                _ts("p.closed_at").alias("closed_at"),
                "_ingested_at", "_run_id",
            )
            .withColumn("is_bug", F.exists("labels", lambda lbl: lbl.rlike("bug|regression|p0|p1")))
            .withColumn("is_feature_request", F.exists("labels", lambda lbl: lbl.rlike("feature|enhancement"))))


def build_comments(env: DataFrame) -> DataFrame:
    p = env.select("repo", "_run_id", _ts("_ingested_at").alias("_ingested_at"),
                   F.from_json("payload", COMMENT_CONTRACT).alias("p"))
    number = F.regexp_extract("p.issue_url", r"/issues/(\d+)$", 1)
    return (p.where(F.col("p.id").isNotNull())
            .select(
                F.col("p.id").alias("comment_id"),
                F.concat_ws("#", "repo", number).alias("issue_key"),
                "repo",
                F.col("p.user.login").alias("author_login"),
                F.col("p.user.type").alias("author_type"),
                F.col("p.author_association").alias("author_association"),
                F.col("p.author_association").isin(MAINTAINER_ASSOC).alias("is_maintainer"),
                F.coalesce(F.length("p.body"), F.lit(0)).alias("body_len"),
                _ts("p.created_at").alias("created_at"),
                _ts("p.updated_at").alias("updated_at"),
                "_ingested_at", "_run_id",
            ))


def _merge(spark: SparkSession, table_uri: str, new: DataFrame, key: str) -> DataFrame:
    prev = read_table(spark, table_uri)
    both = new if prev is None else prev.unionByName(new, allowMissingColumns=True)
    return _latest(both, key)


def run(spark: SparkSession, cfg: Config, run_id: str, full_refresh: bool = False) -> dict[str, Any]:
    state_uri = f"{cfg.state_root.rstrip('/')}/silver_state.json"
    state = {} if full_refresh else storage.read_json(state_uri, default={})
    report: dict[str, Any] = {"run_id": run_id, "tables": {}}
    max_dates = []
    for entity, contract, builder, key in (
        ("issues", ISSUE_CONTRACT, build_issues, "issue_id"),
        ("issue_comments", COMMENT_CONTRACT, build_comments, "comment_id"),
    ):
        # Re-read the last processed day too: a day can receive several ingest runs.
        env = read_bronze(spark, cfg, entity, state.get("last_ingest_date"))
        if env is None or env.limit(1).count() == 0:
            report["tables"][entity] = {"new_bronze_rows": 0, "skipped": True}
            continue
        env = env.cache()
        drift = schema_drift(env, contract, entity)
        batch = builder(env)
        table_uri = f"{cfg.silver}/{entity}"
        merged = batch if full_refresh else _merge(spark, table_uri, batch, key)
        rows = publish_table(merged, table_uri, run_id, coalesce=4)
        max_dates.append(env.agg(F.max("ingest_date")).first()[0])
        report["tables"][entity] = {"new_bronze_rows": env.count(), "silver_rows": rows,
                                    "schema_drift_new_fields": drift}
        if drift:
            log.warning("schema drift in %s: %s", entity, drift)
        env.unpersist()
    if max_dates:
        storage.write_json(state_uri, {"last_ingest_date": max(max_dates), "run_id": run_id})
    return report
