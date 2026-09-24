"""Load gold snapshots from S3 into Redshift Serverless via the Redshift Data API.

Uses the Data API (no JDBC drivers, no open ports, IAM auth) so it runs the same
from Airflow, Lambda, or a laptop. Env vars:
  REDSHIFT_WORKGROUP, REDSHIFT_DATABASE (default: dev), REDSHIFT_COPY_ROLE_ARN
"""
from __future__ import annotations

import glob
import logging
import os
import time
from typing import Any

from signals.config import REPO_ROOT, Config
from signals.snapshots import current_version
from signals.warehouse.load_duckdb import GOLD_TABLES

log = logging.getLogger(__name__)


def _split(sql: str) -> list[str]:
    stmts, buf = [], []
    for line in sql.splitlines():
        if line.strip().startswith("--"):
            continue
        buf.append(line)
        if line.rstrip().endswith(";"):
            stmts.append("\n".join(buf).strip())
            buf = []
    return [s for s in stmts if s]


class DataAPI:
    def __init__(self, workgroup: str, database: str):
        import boto3

        self.c = boto3.client("redshift-data")
        self.wg, self.db = workgroup, database

    def batch(self, sqls: list[str], timeout_s: int = 900) -> None:
        # batch_execute_statement runs the statements as ONE transaction.
        rid = self.c.batch_execute_statement(WorkgroupName=self.wg, Database=self.db, Sqls=sqls)["Id"]
        t0 = time.time()
        while True:
            d = self.c.describe_statement(Id=rid)
            if d["Status"] == "FINISHED":
                return
            if d["Status"] in ("FAILED", "ABORTED"):
                raise RuntimeError(f"Redshift statement failed: {d.get('Error')}")
            if time.time() - t0 > timeout_s:
                raise TimeoutError(rid)
            time.sleep(2)


def run(cfg: Config) -> dict[str, Any]:
    if not cfg.is_s3:
        raise RuntimeError("Redshift load needs SIGNALS_STORAGE_ROOT=s3://...")
    api = DataAPI(os.environ["REDSHIFT_WORKGROUP"], os.environ.get("REDSHIFT_DATABASE", "dev"))
    role = os.environ["REDSHIFT_COPY_ROLE_ARN"]
    api.batch(_split(open(REPO_ROOT / "sql/redshift/ddl.sql").read()))
    template = open(REPO_ROOT / "sql/redshift/load.sql").read()
    loaded = {}
    for t in GOLD_TABLES:
        v = current_version(f"{cfg.gold}/{t}")
        uri = f"{cfg.gold}/{t}/v={v}/"
        # DROP/CREATE staging must run outside the swap transaction; the rest is atomic.
        stmts = _split(template.format(table=t, s3_uri=uri, iam_role=role))
        stmts = [s for s in stmts if s.upper() not in ("BEGIN;", "COMMIT;")]
        api.batch(stmts)
        loaded[t] = uri
        log.info("loaded gold.%s from %s", t, uri)
    marts = []
    for f in sorted(glob.glob(str(REPO_ROOT / "sql/marts/*.sql"))):
        marts += _split(open(f).read())
    api.batch(marts)
    return {"loaded": loaded}
