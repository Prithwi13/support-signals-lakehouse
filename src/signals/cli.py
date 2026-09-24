"""Command-line entry point. Every Airflow task (and Glue job) calls one stage.

    python -m signals ingest
    python -m signals silver [--full-refresh]
    python -m signals dq --layer silver
    python -m signals gold
    python -m signals dq --layer gold
    python -m signals ml
    python -m signals load --target duckdb|redshift
    python -m signals all            # everything, in order (local dev)
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone

from signals import storage
from signals.config import load_config

log = logging.getLogger("signals")


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="signals")
    ap.add_argument("stage", choices=["ingest", "silver", "gold", "dq", "ml", "load", "all"])
    ap.add_argument("--run-id", default=None, help="defaults to current UTC timestamp; Airflow passes its run id")
    ap.add_argument("--layer", choices=["silver", "gold"], help="for the dq stage")
    ap.add_argument("--full-refresh", action="store_true", 
                    help="rebuild silver from all bronze and gold SCD2 from scratch")
    ap.add_argument("--target", choices=["duckdb", "redshift"], default="duckdb")
    ap.add_argument("--cloudwatch", action="store_true", help="publish DQ metrics to CloudWatch")
    ap.add_argument("--snapshot-at", default=None, help="'YYYY-MM-DD HH:MM:SS' UTC; defaults to now")
    a = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    cfg = load_config()
    run_id = a.run_id or _run_id()
    snapshot_at = a.snapshot_at or datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    stages = (["ingest", "silver", "dq:silver", "gold", "dq:gold", "ml", "load"] if a.stage == "all"
              else [f"dq:{a.layer or 'gold'}" if a.stage == "dq" else a.stage])

    summary: dict = {"run_id": run_id, "stages": {}}
    for st in stages:
        t0 = time.time()
        if st == "ingest":
            from signals.ingest import github_issues

            r = github_issues.run(cfg, run_id)
            out = {"records": sum(x.records for x in r.results), "rate_limit_remaining": r.rate_limit_remaining,
                   "by_entity": [{"repo": x.repo, "entity": x.entity, "records": x.records, "pages": x.pages,
                                  "stop": x.stopped_reason} for x in r.results]}
        elif st == "load":
            if a.target == "duckdb":
                from signals.warehouse import load_duckdb as loader
            else:
                from signals.warehouse import load_redshift as loader
            out = loader.run(cfg)
        else:
            from signals.transform.spark import get_spark

            spark = get_spark()
            if st == "silver":
                from signals.transform import silver

                out = silver.run(spark, cfg, run_id, full_refresh=a.full_refresh)
            elif st == "gold":
                from signals.transform import gold

                out = gold.run(spark, cfg, run_id, snapshot_at, full_refresh=a.full_refresh)
            elif st == "ml":
                from signals.ml import features

                out = features.run(spark, cfg, run_id)
            else:
                from signals.quality import checks

                res = checks.run(spark, cfg, run_id, tables=[st.split(":")[1]], cloudwatch=a.cloudwatch)
                out = {"checks": len(res), "failed": [f"{r.table}.{r.name}" for r in res if not r.passed]}
        out["seconds"] = round(time.time() - t0, 1)
        summary["stages"][st] = out
        log.info("stage %s done: %s", st, json.dumps(out, default=str)[:800])

    storage.write_json(f"{cfg.state_root.rstrip('/')}/runs/{run_id}/summary_{a.stage}.json", summary)
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
