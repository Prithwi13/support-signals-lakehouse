"""Spark-layer tests: silver dedup/drift, SCD2 behaviour, DQ, and a full end-to-end run."""
from datetime import datetime, timedelta, timezone

import duckdb
import pytest
from fake_github import START, FakeGitHub, iso

from signals import storage
from signals.ingest import github_issues as gi
from signals.quality import checks
from signals.transform import gold, silver
from signals.transform.spark import read_table
from signals.warehouse import load_duckdb

NOW = datetime(2025, 6, 1, tzinfo=timezone.utc)


def _client(fake):
    return gi.GitHubClient(None, session=fake, sleep=lambda s: None)


def _bronze_record(repo, payload, run_id, ingested_at):
    return {"_run_id": run_id, "_ingested_at": ingested_at, "_source": "test", "repo": repo, "payload": payload}


def _issue(i, updated, state="open", labels=(), **extra):
    return {"id": i, "number": i, "title": f"s3 bucket issue {i}", "state": state, "user": {"login": "u"},
            "labels": [{"name": x} for x in labels], "created_at": "2025-01-01T00:00:00Z",
            "updated_at": updated, "closed_at": None if state == "open" else updated, **extra}


def test_silver_dedup_late_arrival_pr_filter_and_drift(spark, cfg):
    repo = "aws/aws-cli"
    base = f"{cfg.bronze}/issues/repo=aws__aws-cli"
    # Day 1 file has the NEWEST version of issue 1; day 2 file re-delivers an OLDER version (late arrival).
    storage.write_jsonl_gz(f"{base}/ingest_date=2025-02-01/a.jsonl.gz", [
        _bronze_record(repo, _issue(1, "2025-02-01T10:00:00Z", "closed"), "a", "2025-02-01T11:00:00Z"),
        _bronze_record(repo, _issue(2, "2025-02-01T09:00:00Z", pull_request={"url": "x"}), "a", "2025-02-01T11:00:00Z"),
    ])
    storage.write_jsonl_gz(f"{base}/ingest_date=2025-02-02/b.jsonl.gz", [
        _bronze_record(repo, _issue(1, "2025-01-20T10:00:00Z", "open"), "b", "2025-02-02T11:00:00Z"),
        _bronze_record(repo, _issue(3, "2025-02-02T09:00:00Z", brand_new_field=1), "b", "2025-02-02T11:00:00Z"),
    ])
    rep = silver.run(spark, cfg, "s1")
    df = read_table(spark, f"{cfg.silver}/issues").orderBy("issue_id").collect()
    assert [r.issue_id for r in df] == [1, 3]                 # PR #2 dropped
    assert df[0].state == "closed"                            # newest version wins despite arriving first
    assert rep["tables"]["issues"]["schema_drift_new_fields"] == {"brand_new_field": 1}


def test_scd2_insert_change_and_late(spark):
    s = spark.createDataFrame
    cols = "issue_id long, issue_key string, state string, labels_str string, service_slug string, " \
           "milestone string, assignee_count int, updated_at timestamp"
    t0, t1 = datetime(2025, 1, 1), datetime(2025, 1, 5)
    v1, st = gold.build_dim_issue_scd2(None, s([(1, "r#1", "open", "bug", "s3", None, 0, t0),
                                                (2, "r#2", "open", "", "ec2", None, 0, t0)], cols), "run1")
    assert st["inserted_new"] == 2
    snap2 = s([(1, "r#1", "closed", "bug", "s3", None, 0, t1),                      # changed
               (2, "r#2", "closed", "", "ec2", None, 0, datetime(2024, 12, 1)),     # late/out-of-order
               (3, "r#3", "open", "", "iam", None, 0, t1)], cols)                   # new
    v2, st2 = gold.build_dim_issue_scd2(v1, snap2, "run2")
    assert st2 == {"inserted_new": 1, "changed": 1, "late_ignored": 1}
    rows = {(r.issue_id, r.is_current): r for r in v2.collect()}
    assert rows[(1, False)].valid_to == t1 and rows[(1, True)].state == "closed"
    assert rows[(2, True)].state == "open"                    # late update did not overwrite
    assert v2.count() == 4
    # idempotent: re-applying the same snapshot changes nothing
    v3, st3 = gold.build_dim_issue_scd2(v2, snap2, "run3")
    assert st3["changed"] == 0 and v3.count() == 4


def test_service_mapping(spark, cfg):
    df = spark.createDataFrame([(["@aws-cdk/aws-lambda"], "x"), ([], "Presigned S3 url broken"),
                                (["client-dynamodb"], "x"), ([], "something else"),
                                (["@aws-cdk/aws-lambda-nodejs"], "x"), (["@aws-cdk/aws-eks-v2-alpha"], "x"),
                                (["@aws-cdk/aws-elasticloadbalancingv2"], "x")],
                               "labels array<string>, title string")
    slug = gold.normalize_slug(gold.service_expr(cfg.service_map), cfg.service_map)
    out = [r.s for r in df.select(slug.alias("s")).collect()]
    assert out == ["lambda", "s3", "dynamodb", "unknown", "lambda", "eks", "elb"]


def test_end_to_end_two_runs(spark, cfg):
    fake = FakeGitHub().seed(["aws/aws-cdk", "aws/aws-cli"], 150, START)
    gi.run(cfg, "run1", _client(fake), now=NOW)
    silver.run(spark, cfg, "run1")
    checks.run(spark, cfg, "run1", tables=["silver"])
    rep1 = gold.run(spark, cfg, "run1", "2025-06-01 00:00:00")
    checks.run(spark, cfg, "run1", tables=["gold"])

    n_issues = sum(1 for r in ["aws/aws-cdk", "aws/aws-cli"] for i in fake.issues[r] if "pull_request" not in i)
    assert rep1["tables"]["fact_issue"] == n_issues
    assert rep1["scd2"]["inserted_new"] == n_issues

    # Day 2: close three issues and relabel one; SCD2 must add exactly 4 versions.
    later = NOW + timedelta(days=1)
    open_ones = [i for i in fake.issues["aws/aws-cdk"] if i["state"] == "open" and "pull_request" not in i][:3]
    for i in open_ones:
        fake.touch("aws/aws-cdk", i["number"], later, state="closed", closed_at=iso(later))
    target = next(i for i in fake.issues["aws/aws-cli"] if "pull_request" not in i)
    fake.touch("aws/aws-cli", target["number"], later, labels=[{"name": "p0"}, {"name": "bug"}])

    gi.run(cfg, "run2", _client(fake), now=later)
    silver.run(spark, cfg, "run2")
    rep2 = gold.run(spark, cfg, "run2", "2025-06-02 00:00:00")
    checks.run(spark, cfg, "run2", tables=["gold"])
    assert rep2["scd2"]["changed"] == 4 and rep2["scd2"]["inserted_new"] == 0
    assert rep2["tables"]["dim_issue_scd2"] == n_issues + 4

    # Airflow retry of the same run: must be idempotent and must not clobber its own input.
    silver.run(spark, cfg, "run2")
    retry = gold.run(spark, cfg, "run2", "2025-06-02 00:00:00")
    assert retry["scd2"] == {"inserted_new": 0, "changed": 0, "late_ignored": 0}
    assert retry["tables"] == rep2["tables"]

    from signals.ml import features

    ml = features.run(spark, cfg, "run2", k=5)
    assert ml["tables"]["issue_topics"] == n_issues
    assert "roc_auc" in ml["escalation_model"] or "skipped" in ml["escalation_model"]

    out = load_duckdb.run(cfg)
    assert out["rows"]["fact_issue"] == n_issues
    con = duckdb.connect(out["warehouse"], read_only=True)
    assert con.execute("SELECT COUNT(*) FROM marts.service_health_monthly").fetchone()[0] > 0
    assert con.execute("SELECT SUM(open_issues) FROM marts.backlog_by_service").fetchone()[0] > 0
    assert con.execute("SELECT COUNT(*) FROM marts.open_backlog_asof_weekly").fetchone()[0] > 0
    assert con.execute("SELECT COUNT(*) FROM marts.repeat_issue_rate").fetchone()[0] > 0


def test_dq_fails_on_duplicate_keys(spark, cfg):
    from signals.transform.spark import publish_table

    df = spark.createDataFrame([(1, "a#1", "a", 1, "t", "open"), (1, "a#1", "a", 1, "t", "open")],
                               "issue_id long, issue_key string, repo string, number long, title string, state string")
    df = df.selectExpr("*", "timestamp'2025-01-01' AS created_at", "timestamp'2025-01-01' AS updated_at",
                       "CAST(NULL AS timestamp) AS closed_at")
    publish_table(df, f"{cfg.silver}/issues", "bad")
    publish_table(spark.createDataFrame([(1, "a#1", datetime(2025, 1, 1))],
                                        "comment_id long, issue_key string, created_at timestamp"),
                  f"{cfg.silver}/issue_comments", "bad")
    with pytest.raises(checks.DataQualityError, match="unique:issue_id"):
        checks.run(spark, cfg, "bad", tables=["silver"])
