"""Load published gold snapshots into a local DuckDB warehouse and build the marts.

DuckDB plays the role Redshift plays on AWS: same star schema, same mart SQL.
The whole load runs in one transaction, so a failed load leaves the previous
warehouse state untouched.
"""
from __future__ import annotations

import glob
import logging
import os
from typing import Any

from signals.config import REPO_ROOT, Config
from signals.snapshots import current_version

log = logging.getLogger(__name__)
GOLD_TABLES = ["dim_date", "dim_repo", "dim_service", "dim_issue_scd2", "bridge_issue_label", "fact_issue"]


def warehouse_path(cfg: Config) -> str:
    return os.environ.get("SIGNALS_WAREHOUSE", os.path.join(cfg.storage_root, "warehouse.duckdb"))


def run(cfg: Config) -> dict[str, Any]:
    import duckdb

    if cfg.is_s3:
        raise RuntimeError("DuckDB loader is for local runs; on AWS use load_redshift")
    path = warehouse_path(cfg)
    con = duckdb.connect(path)
    counts: dict[str, Any] = {}
    try:
        con.execute("BEGIN")
        con.execute(open(REPO_ROOT / "sql/duckdb/ddl.sql").read())
        for t in GOLD_TABLES:
            v = current_version(f"{cfg.gold}/{t}")
            if v is None:
                raise RuntimeError(f"gold.{t} has no published snapshot")
            files = f"{cfg.gold}/{t}/v={v}/**/*.parquet"
            cols = [r[0] for r in con.execute(f"DESCRIBE gold.{t}").fetchall()]
            con.execute(f"INSERT INTO gold.{t} SELECT {', '.join(cols)} FROM read_parquet('{files}')")
            counts[t] = con.execute(f"SELECT COUNT(*) FROM gold.{t}").fetchone()[0]
        for sql_file in sorted(glob.glob(str(REPO_ROOT / "sql/marts/*.sql"))):
            con.execute(open(sql_file).read())
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()
    log.info("loaded %s into %s", counts, path)
    return {"warehouse": path, "rows": counts}
