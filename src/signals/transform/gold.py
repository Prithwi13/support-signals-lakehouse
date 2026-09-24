"""Silver -> Gold star schema.

Tables
------
dim_date          one row per calendar day (date_key = yyyymmdd)
dim_repo          one row per source repo
dim_service       one row per AWS service (service_key 0 = "unknown")
dim_issue_scd2    Type-2 slowly changing dimension: a new row every time an issue's
                  tracked attributes (state, labels, service, milestone, assignees)
                  change between runs; old rows are closed with valid_to.
bridge_issue_label  issue <-> current label (many-to-many)
fact_issue        accumulating-snapshot fact, one row per issue, with its lifecycle
                  milestones (created, first maintainer response, closed) and
                  derived durations / SLA flags.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from signals.config import Config
from signals.transform.spark import publish_table, read_table

OPEN_END = "9999-12-31 00:00:00"
SCD2_TRACKED = ["state", "labels_str", "service_slug", "milestone", "assignee_count"]
# Column order matters: Redshift COPY ... FORMAT PARQUET maps columns by position.
SCD2_COLUMNS = ["issue_id", "issue_key", *SCD2_TRACKED, "valid_from", "valid_to", "is_current", "attr_hash", "_run_id"]


def surrogate(*cols: Column) -> Column:
    """Deterministic surrogate key: stable across runs and environments."""
    return F.abs(F.xxhash64(*cols))


def date_key(c: Column) -> Column:
    return F.date_format(c, "yyyyMMdd").cast("int")


# ---------------------------------------------------------------- service mapping
def service_expr(service_map: dict[str, Any]) -> Column:
    """Return a Column resolving an issue's AWS service from labels, then title keywords."""
    label_hits = []
    def extractor(pattern: str):
        return lambda lbl: F.regexp_extract(lbl, pattern, 1)  # 1-arg lambda: Spark passes (elem, idx) to 2-arg ones

    for pat in service_map.get("label_patterns", []):
        hit = F.filter(F.transform("labels", extractor(pat)), lambda x: x != "")
        label_hits.append(F.element_at(hit, 1))
    from_labels = F.coalesce(*label_hits) if label_hits else F.lit(None)

    title = F.lower(F.coalesce(F.col("title"), F.lit("")))
    kw = None
    for slug, pat in service_map.get("keyword_patterns", {}).items():
        cond = title.rlike(pat)
        kw = F.when(cond, F.lit(slug)) if kw is None else kw.when(cond, F.lit(slug))
    from_title = kw.otherwise(F.lit(None)) if kw is not None else F.lit(None)
    return F.coalesce(from_labels, from_title, F.lit("unknown"))


def normalize_slug(c: Column, service_map: dict[str, Any] | None = None) -> Column:
    """Native-Spark slug cleanup (no Python UDF, so it runs in the JVM at full speed):
    lowercase -> strip sub-module suffixes (twice, e.g. "-v2-alpha") -> apply aliases."""
    sm = service_map or {}
    out = F.regexp_replace(F.regexp_replace(F.lower(c), "[^a-z0-9]+", "-"), "^-+|-+$", "")
    if sm.get("strip_suffix_pattern"):
        for _ in range(2):
            out = F.regexp_replace(out, sm["strip_suffix_pattern"], "")
    aliases = sm.get("aliases") or {}
    if aliases:
        m = F.create_map(*[F.lit(x) for kv in aliases.items() for x in kv])
        out = F.coalesce(m[out], out)
    return out


# ---------------------------------------------------------------- dimensions
def build_dim_date(spark: SparkSession, start: str, end: str) -> DataFrame:
    d = spark.sql(f"SELECT explode(sequence(to_date('{start}'), to_date('{end}'), interval 1 day)) AS d")
    return d.select(
        date_key(F.col("d")).alias("date_key"), F.col("d").alias("calendar_date"),
        F.year("d").alias("year"), F.quarter("d").alias("quarter"), F.month("d").alias("month"),
        F.date_format("d", "yyyy-MM").alias("year_month"), F.weekofyear("d").alias("iso_week"),
        F.dayofweek("d").alias("day_of_week"), F.dayofweek("d").isin(1, 7).alias("is_weekend"),
    )


def build_dim_service(issues: DataFrame) -> DataFrame:
    slugs = issues.select("service_slug").distinct()
    return (slugs.select(
        F.when(F.col("service_slug") == "unknown", F.lit(0)).otherwise(surrogate(F.col("service_slug")))
        .alias("service_key"),
        F.col("service_slug"),
        F.upper(F.regexp_replace("service_slug", "-", " ")).alias("service_name"))
        .unionByName(issues.sparkSession.createDataFrame([(0, "unknown", "UNKNOWN")],
                                                         "service_key long, service_slug string, service_name string"))
        .dropDuplicates(["service_key"]))


def build_dim_repo(issues: DataFrame) -> DataFrame:
    return (issues.select("repo").distinct()
            .select(surrogate(F.col("repo")).alias("repo_key"), "repo",
                    F.split("repo", "/")[0].alias("owner"), F.split("repo", "/")[1].alias("repo_name")))


def build_dim_issue_scd2(prev: DataFrame | None, snapshot: DataFrame, run_id: str) -> tuple[DataFrame, dict]:
    """Merge today's snapshot into the SCD2 dimension.

    * new issue                         -> insert version (valid_from = updated_at)
    * tracked attributes changed        -> close current row, insert new version
    * unchanged                         -> no-op
    * late/out-of-order (updated_at <= current valid_from) -> ignored (idempotent).
      A logic change (same updated_at, different derived value) also lands here;
      that case needs a full refresh, see run().
    """
    snap = (snapshot.select("issue_id", "issue_key", *SCD2_TRACKED, F.col("updated_at").alias("valid_from"))
            .withColumn("attr_hash", F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("∅"))
                                                                 for c in SCD2_TRACKED]), 256)))
    open_end = F.to_timestamp(F.lit(OPEN_END))
    if prev is None:
        out = (snap.withColumn("valid_to", open_end).withColumn("is_current", F.lit(True))
               .withColumn("_run_id", F.lit(run_id)))
        return out, {"inserted_new": out.count(), "changed": 0, "late_ignored": 0}

    cur = prev.where("is_current")
    hist = prev.where("NOT is_current")
    j = snap.alias("s").join(cur.alias("c"), "issue_id", "full_outer")

    is_new = F.col("c.attr_hash").isNull()
    is_gone = F.col("s.attr_hash").isNull()
    changed = (~is_new & ~is_gone & (F.col("s.attr_hash") != F.col("c.attr_hash"))
               & (F.col("s.valid_from") > F.col("c.valid_from")))
    late = (~is_new & ~is_gone & (F.col("s.attr_hash") != F.col("c.attr_hash"))
            & (F.col("s.valid_from") <= F.col("c.valid_from")))

    cols = ["issue_id", "issue_key", *SCD2_TRACKED, "valid_from", "attr_hash"]
    keep_current = j.where(~is_new & ~changed).select(
        "issue_id", *[F.col(f"c.{c}").alias(c) for c in cols[1:]], "c.valid_to", "c.is_current", "c._run_id")
    closed = j.where(changed).select(
        "issue_id", *[F.col(f"c.{c}").alias(c) for c in cols[1:]],
        F.col("s.valid_from").alias("valid_to"), F.lit(False).alias("is_current"), "c._run_id")
    inserted = j.where(is_new | changed).select(
        "issue_id", *[F.col(f"s.{c}").alias(c) for c in cols[1:]],
        open_end.alias("valid_to"), F.lit(True).alias("is_current"), F.lit(run_id).alias("_run_id"))

    out = hist.unionByName(keep_current).unionByName(closed).unionByName(inserted)
    stats = j.agg(F.sum(is_new.cast("int")).alias("inserted_new"), F.sum(changed.cast("int")).alias("changed"),
                  F.sum(late.cast("int")).alias("late_ignored")).first().asDict()
    return out, {k: int(v or 0) for k, v in stats.items()}


# ---------------------------------------------------------------- fact
def first_maintainer_response(issues: DataFrame, comments: DataFrame,
                              responder_assoc: list[str] | None = None) -> DataFrame:
    """Earliest reply from a maintainer-like account that isn't the issue author or a bot."""
    assoc = responder_assoc or ["OWNER", "MEMBER", "COLLABORATOR"]
    c = comments.where(F.col("author_association").isin(assoc)
                       & (F.coalesce("author_type", F.lit("User")) != "Bot")
                       & ~F.col("author_login").endswith("[bot]"))
    j = c.alias("c").join(issues.select("issue_key", "author_login").alias("i"), "issue_key")
    return (j.where(F.col("c.author_login") != F.col("i.author_login"))
            .groupBy("issue_key").agg(F.min("c.created_at").alias("first_response_at")))


def build_fact_issue(issues: DataFrame, comments: DataFrame, sla: dict, snapshot_at: str) -> DataFrame:
    fr = first_maintainer_response(issues, comments, sla.get("responder_associations"))
    hours = lambda a, b: (F.unix_timestamp(b) - F.unix_timestamp(a)) / 3600.0  # noqa: E731
    now = F.to_timestamp(F.lit(snapshot_at))
    f = issues.join(fr, "issue_key", "left")
    closed = F.col("state") == "closed"
    return f.select(
        "issue_id", "issue_key",
        surrogate(F.col("repo")).alias("repo_key"),
        F.when(F.col("service_slug") == "unknown", F.lit(0)).otherwise(surrogate(F.col("service_slug")))
        .alias("service_key"),
        date_key(F.col("created_at")).alias("created_date_key"),
        date_key(F.col("closed_at")).alias("closed_date_key"),
        "created_at", "first_response_at", "closed_at", "state", closed.alias("is_closed"),
        "is_bug", "is_feature_request", F.col("comment_count").cast("int").alias("comment_count"),
        F.col("reaction_count").cast("int").alias("reaction_count"),
        F.col("assignee_count").cast("int").alias("assignee_count"), F.col("body_len").cast("int").alias("body_len"),
        "has_code_block", "author_association",
        F.round(hours(F.col("created_at"), F.col("first_response_at")), 2).alias("hours_to_first_response"),
        F.round(F.when(closed, hours(F.col("created_at"), F.col("closed_at"))), 2).alias("hours_to_close"),
        F.round(F.when(~closed, hours(F.col("created_at"), now)), 2).alias("open_age_hours"),
        # SLA breached = no maintainer reply within N hours (or none yet and N hours already passed)
        F.when(F.col("first_response_at").isNotNull(),
               hours(F.col("created_at"), F.col("first_response_at")) > sla["first_response_hours"])
        .otherwise(hours(F.col("created_at"), now) > sla["first_response_hours"]).alias("first_response_sla_breached"),
        now.alias("snapshot_at"),
    )


def enrich_issues(issues: DataFrame, cfg: Config) -> DataFrame:
    return (issues.withColumn("service_slug", normalize_slug(service_expr(cfg.service_map), cfg.service_map))
            .withColumn("labels_str", F.array_join(F.array_sort("labels"), ",")))


def run(spark: SparkSession, cfg: Config, run_id: str, snapshot_at: str,
        full_refresh: bool = False) -> dict[str, Any]:
    """Build gold. full_refresh=True rebuilds the SCD2 history from scratch: use it after a
    change to transformation logic (e.g. new service mapping) so old versions are recomputed,
    since an in-place merge would treat the new values as out-of-order updates and skip them."""
    issues = read_table(spark, f"{cfg.silver}/issues")
    comments = read_table(spark, f"{cfg.silver}/issue_comments")
    if issues is None:
        raise RuntimeError("silver.issues is empty; run ingest + silver first")
    if comments is None:
        comments = spark.createDataFrame([], "comment_id long, issue_key string, repo string, author_login string, "
                                             "author_type string, author_association string, is_maintainer boolean, "
                                             "body_len int, created_at timestamp, updated_at timestamp")
    issues = enrich_issues(issues, cfg).cache()
    g = cfg.gold
    rep: dict[str, Any] = {"run_id": run_id, "tables": {}}

    bounds = issues.agg(F.min("created_at"), F.max("updated_at")).first()
    snap_dt = datetime.strptime(snapshot_at[:19], "%Y-%m-%d %H:%M:%S")
    start = bounds[0].strftime("%Y-01-01")
    end = max(bounds[1], snap_dt).strftime("%Y-12-31")
    rep["tables"]["dim_date"] = publish_table(build_dim_date(spark, start, end), f"{g}/dim_date", run_id, coalesce=1)
    rep["tables"]["dim_repo"] = publish_table(build_dim_repo(issues), f"{g}/dim_repo", run_id, coalesce=1)
    rep["tables"]["dim_service"] = publish_table(build_dim_service(issues), f"{g}/dim_service", run_id, coalesce=1)

    prev_scd = None if full_refresh else read_table(spark, f"{g}/dim_issue_scd2")
    scd, scd_stats = build_dim_issue_scd2(prev_scd, issues, run_id)
    rep["tables"]["dim_issue_scd2"] = publish_table(scd.select(*SCD2_COLUMNS), f"{g}/dim_issue_scd2", run_id,
                                                    coalesce=4)
    rep["scd2"] = scd_stats

    bridge = issues.select("issue_id", F.explode("labels").alias("label")).distinct()
    rep["tables"]["bridge_issue_label"] = publish_table(bridge, f"{g}/bridge_issue_label", run_id, coalesce=2)

    fact = build_fact_issue(issues, comments, cfg.sla, snapshot_at)
    rep["tables"]["fact_issue"] = publish_table(fact, f"{g}/fact_issue", run_id, coalesce=4)
    issues.unpersist()
    return rep

