"""Declarative data quality checks (config/quality.yaml) evaluated with Spark.

Each check returns a result with a measured value, so results double as metrics:
they're written to a JSON report and, on AWS, pushed to CloudWatch where alarms
fire on failures. Severity 'error' fails the pipeline; 'warn' only alerts.

(Same idea as AWS Deequ's VerificationSuite; kept dependency-free so it runs on
any Spark version. See README for swapping in PyDeequ.)
"""
from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from signals import storage
from signals.config import Config
from signals.transform.spark import read_table

log = logging.getLogger(__name__)


@dataclass
class CheckResult:
    table: str
    check: str
    name: str
    severity: str
    passed: bool
    value: float | None
    detail: str = ""


class DataQualityError(RuntimeError):
    pass


def _table_uri(cfg: Config, name: str) -> str:
    layer, table = name.split(".", 1)
    return f"{getattr(cfg, layer)}/{table}"


def evaluate(df: DataFrame, spec: dict[str, Any], table: str, resolve) -> CheckResult:
    c = spec["check"]
    sev = spec.get("severity", "error")
    name = spec.get("name", c + ("" if "columns" not in spec else ":" + ",".join(spec["columns"]))
                    + (":" + spec["column"] if "column" in spec else ""))
    total = df.count()

    def res(passed: bool, value: float | None, detail: str = "") -> CheckResult:
        return CheckResult(table, c, name, sev, bool(passed), None if value is None else float(value), detail)

    if c == "row_count_min":
        return res(total >= spec["min"], total, f"rows={total}")
    if c == "not_null":
        cond = " OR ".join(f"{col} IS NULL" for col in spec["columns"])
        bad = df.where(cond).count()
        return res(bad == 0, bad, f"{bad} rows with nulls")
    if c == "unique":
        dup = df.groupBy(*spec["columns"]).count().where("count > 1").count()
        return res(dup == 0, dup, f"{dup} duplicated keys")
    if c == "accepted_values":
        bad = df.where(~F.col(spec["column"]).isin(spec["values"]) | F.col(spec["column"]).isNull()).count()
        return res(bad == 0, bad, f"{bad} rows outside {spec['values']}")
    if c == "expression":
        bad = df.where(f"NOT ({spec['expr']})").count()
        return res(bad == 0, bad, f"{bad} rows violate: {spec['expr']}")
    if c == "freshness":
        latest = df.agg(F.max(spec["column"])).first()[0]
        if latest is None:
            return res(False, None, "no data")
        age_h = (datetime.now(timezone.utc).replace(tzinfo=None) - latest).total_seconds() / 3600
        return res(age_h <= spec["max_age_hours"], round(age_h, 2), f"newest record {age_h:.1f}h old")
    if c == "max_null_fraction":
        frac = df.where(F.col(spec["column"]).isNull()).count() / total if total else 0
        return res(frac <= spec["max_fraction"], round(frac, 4), f"null fraction {frac:.2%}")
    if c == "max_fraction_equal":
        frac = df.where(F.col(spec["column"]) == F.lit(spec["value"])).count() / total if total else 0
        return res(frac <= spec["max_fraction"], round(frac, 4), f"{frac:.2%} equal to {spec['value']}")
    if c == "foreign_key":
        ref = resolve(spec["ref_table"]).select(F.col(spec["ref_column"]).alias("_ref")).distinct()
        orphans = df.join(ref, F.col(spec["column"]) == F.col("_ref"), "left_anti").count()
        return res(orphans == 0, orphans, f"{orphans} orphan rows vs {spec['ref_table']}")
    if c == "scd2_one_current":
        bad = (df.groupBy(spec["key"]).agg(F.sum(F.col("is_current").cast("int")).alias("n"))
               .where("n <> 1").count())
        return res(bad == 0, bad, f"{bad} keys without exactly one current row")
    if c == "scd2_no_overlap":
        w = Window.partitionBy(spec["key"]).orderBy("valid_from")
        bad = (df.withColumn("_next", F.lead("valid_from").over(w))
               .where("_next IS NOT NULL AND valid_to > _next").count())
        return res(bad == 0, bad, f"{bad} overlapping validity ranges")
    raise ValueError(f"unknown check type: {c}")


def publish_cloudwatch(results: list[CheckResult], namespace: str, run_id: str) -> None:
    import boto3

    cw = boto3.client("cloudwatch")
    data = []
    for r in results:
        dims = [{"Name": "Table", "Value": r.table}, {"Name": "Check", "Value": r.name[:250]}]
        data.append({"MetricName": "CheckFailed", "Dimensions": dims, "Value": 0.0 if r.passed else 1.0})
        if r.value is not None:
            data.append({"MetricName": "CheckValue", "Dimensions": dims, "Value": r.value})
    failed = sum(1 for r in results if not r.passed and r.severity == "error")
    data.append({"MetricName": "ErrorChecksFailed", "Value": float(failed)})
    for i in range(0, len(data), 20):  # PutMetricData batch limit
        cw.put_metric_data(Namespace=namespace, MetricData=data[i:i + 20])
    log.info("published %d DQ metrics to CloudWatch (%s) for run %s", len(data), namespace, run_id)


def run(spark: SparkSession, cfg: Config, run_id: str, tables: list[str] | None = None,
        cloudwatch: bool = False) -> list[CheckResult]:
    cache: dict[str, DataFrame] = {}

    def resolve(name: str) -> DataFrame:
        if name not in cache:
            df = read_table(spark, _table_uri(cfg, name))
            if df is None:
                raise DataQualityError(f"table {name} has no published snapshot")
            cache[name] = df.cache()
        return cache[name]

    results: list[CheckResult] = []
    for table, specs in cfg.quality_checks.items():
        if tables and not any(table.startswith(t) for t in tables):
            continue
        df = resolve(table)
        for spec in specs:
            r = evaluate(df, spec, table, resolve)
            (log.info if r.passed else log.warning)("DQ %-6s %-24s %-40s %s", "PASS" if r.passed else "FAIL",
                                                    table, r.name, r.detail)
            results.append(r)

    storage.write_json(f"{cfg.state_root.rstrip('/')}/runs/{run_id}/dq_{'_'.join(tables or ['all'])}.json",
                       [asdict(r) for r in results])
    if cloudwatch:
        publish_cloudwatch(results, cfg.quality["cloudwatch_namespace"], run_id)

    fail_on = cfg.quality.get("fail_on", "error")
    blocking = [r for r in results if not r.passed and (
        fail_on == "warn" or (fail_on == "error" and r.severity == "error"))]
    if blocking:
        raise DataQualityError("; ".join(f"{r.table}.{r.name}: {r.detail}" for r in blocking))
    return results
