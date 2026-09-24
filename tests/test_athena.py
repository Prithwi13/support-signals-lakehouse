"""Athena loader: statement rendering and orchestration against a fake Athena client."""
from signals.config import REPO_ROOT
from signals.warehouse import load_athena
from signals.warehouse.load_duckdb import GOLD_TABLES


class FakeAthena:
    def __init__(self):
        self.sql = []

    def start_query_execution(self, QueryString, WorkGroup):
        self.sql.append(QueryString)
        return {"QueryExecutionId": str(len(self.sql))}

    def get_query_execution(self, QueryExecutionId):
        return {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}

    def get_query_results(self, QueryExecutionId):
        return {"ResultSet": {"Rows": [{"Data": [{"VarCharValue": "_col0"}]}, {"Data": [{"VarCharValue": "7"}]}]}}


def test_render_tables_and_views():
    locs = {t: f"s3://b/lake/gold/{t}/v=r1.abc/" for t in GOLD_TABLES}
    tables = load_athena.render(open(REPO_ROOT / "sql/athena/tables.sql").read(), "sig", locs)
    views = load_athena.render(open(REPO_ROOT / "sql/athena/marts.sql").read(), "sig", locs)
    assert len(tables) == len(GOLD_TABLES) and len(views) == 4
    assert all(not s.endswith(";") and "{" not in s for s in tables + views)
    assert "LOCATION 's3://b/lake/gold/fact_issue/v=r1.abc/'" in tables[-1]


def test_run_repoints_locations(monkeypatch, cfg):
    cfg.storage_root = "s3://bucket/lake"
    monkeypatch.setattr(load_athena, "current_version", lambda uri: "r1.abc")
    fake = FakeAthena()
    out = load_athena.run(cfg, client=fake)
    assert out["rows"] == {t: 7 for t in GOLD_TABLES}
    alters = [s for s in fake.sql if s.startswith("ALTER TABLE")]
    assert len(alters) == len(GOLD_TABLES)
    assert "s3://bucket/lake/gold/dim_date/v=r1.abc/" in alters[0]
