"""Register gold snapshots in the Glue Data Catalog and (re)build mart views in Athena.

Serverless and pay-per-query, so it works on AWS Free-plan accounts where Redshift
Serverless isn't available. Env vars: ATHENA_WORKGROUP, ATHENA_DATABASE.
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any

from signals.config import REPO_ROOT, Config
from signals.snapshots import current_version
from signals.warehouse.load_duckdb import GOLD_TABLES
from signals.warehouse.load_redshift import _split

log = logging.getLogger(__name__)


class Athena:
    def __init__(self, workgroup: str, client: Any = None, sleep=time.sleep):
        if client is None:
            import boto3

            client = boto3.client("athena")
        self.c, self.wg, self.sleep = client, workgroup, sleep

    def run(self, sql: str, timeout_s: int = 300) -> str:
        qid = self.c.start_query_execution(QueryString=sql, WorkGroup=self.wg)["QueryExecutionId"]
        t0 = time.time()
        while True:
            st = self.c.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
            if st["State"] == "SUCCEEDED":
                return qid
            if st["State"] in ("FAILED", "CANCELLED"):
                raise RuntimeError(f"Athena query failed: {st.get('StateChangeReason')}\n{sql[:300]}")
            if time.time() - t0 > timeout_s:
                raise TimeoutError(qid)
            self.sleep(1)


def render(template: str, db: str, locations: dict[str, str]) -> list[str]:
    sql = template.format(db=db, **{f"loc_{t}": loc for t, loc in locations.items()})
    return [s.rstrip().rstrip(";") for s in _split(sql)]  # Athena rejects a trailing ';'


def run(cfg: Config, client: Any = None) -> dict[str, Any]:
    if not cfg.is_s3:
        raise RuntimeError("Athena load needs SIGNALS_STORAGE_ROOT=s3://...")
    db = os.environ.get("ATHENA_DATABASE", "support_signals")
    athena = Athena(os.environ.get("ATHENA_WORKGROUP", "support-signals"), client)
    locs = {}
    for t in GOLD_TABLES:
        v = current_version(f"{cfg.gold}/{t}")
        if v is None:
            raise RuntimeError(f"gold.{t} has no published snapshot")
        locs[t] = f"{cfg.gold}/{t}/v={v}/"
    for stmt in render(open(REPO_ROOT / "sql/athena/tables.sql").read(), db, locs):
        athena.run(stmt)
    # Existing tables keep their old LOCATION after CREATE IF NOT EXISTS: re-point them.
    for t, loc in locs.items():
        athena.run(f"ALTER TABLE {db}.{t} SET LOCATION '{loc}'")
    views = render(open(REPO_ROOT / "sql/athena/marts.sql").read(), db, locs)
    for stmt in views:
        athena.run(stmt)
    counts = {}
    for t in GOLD_TABLES:
        qid = athena.run(f"SELECT COUNT(*) FROM {db}.{t}")
        rows = athena.c.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
        counts[t] = int(rows[1]["Data"][0]["VarCharValue"])
    log.info("athena tables %s", counts)
    return {"database": db, "rows": counts, "views": len(views)}
