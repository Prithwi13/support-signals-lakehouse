"""Support-health dashboard over the local warehouse (DuckDB).

    pip install streamlit && streamlit run dashboard/app.py

On AWS the same mart views live in Redshift; point QuickSight at the `marts`
schema for the hosted version (see README > Serving).
"""
from __future__ import annotations

import os

import duckdb
import pandas as pd
import streamlit as st

DB = os.environ.get("SIGNALS_WAREHOUSE", "data/warehouse.duckdb")
st.set_page_config(page_title="AWS Support Signals", layout="wide")


@st.cache_data(ttl=300)
def q(sql: str) -> pd.DataFrame:
    with duckdb.connect(DB, read_only=True) as con:
        return con.execute(sql).df()


st.title("AWS customer-support signals")
st.caption("Public GitHub issues on AWS SDK/CLI/CDK repos, modeled as support cases. "
           f"Snapshot: {q('SELECT max(snapshot_at) s FROM gold.fact_issue').s[0]}")

since = st.sidebar.date_input("Issues opened since", pd.Timestamp("2026-07-01"))
repos = q("SELECT repo FROM gold.dim_repo ORDER BY 1").repo.tolist()
pick = st.sidebar.multiselect("Repos", repos, default=repos)
repo_list = ",".join(f"'{r}'" for r in pick) or "''"
base = f"""FROM gold.fact_issue f JOIN gold.dim_repo r USING (repo_key) JOIN gold.dim_service s USING (service_key)
           WHERE f.created_at >= DATE '{since}' AND r.repo IN ({repo_list})"""

k = q(f"""SELECT COUNT(*) n, median(hours_to_first_response) ttfr, AVG(first_response_sla_breached::INT) breach,
                 SUM((NOT is_closed)::INT) open_n, median(hours_to_close) ttc {base}""").iloc[0]
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Issues opened", f"{int(k.n):,}")
c2.metric("Open now", f"{int(k.open_n or 0):,}")
c3.metric("Median time to first response", f"{(k.ttfr or 0):.1f} h")
c4.metric("First-response SLA breach", f"{(k.breach or 0):.0%}")
c5.metric("Median time to close", f"{(k.ttc or 0) / 24:.1f} d")

left, right = st.columns(2)
with left:
    st.subheader("Volume by service")
    st.bar_chart(q(f"""SELECT service_slug, COUNT(*) issues {base} AND service_slug <> 'unknown'
                      GROUP BY 1 ORDER BY 2 DESC LIMIT 15""").set_index("service_slug"))
with right:
    st.subheader("Median hours to first response, by week")
    st.line_chart(q(f"""SELECT date_trunc('week', f.created_at) AS week_start,
                              median(hours_to_first_response) AS hours {base}
                       GROUP BY 1 ORDER BY 1""").set_index("week_start"))

st.subheader("Open backlog by service (as of each week, from the SCD2 history)")
asof = q("SELECT asof_ts, service_slug, open_issues_asof FROM marts.open_backlog_asof_weekly")
if len(asof):
    top = asof.groupby("service_slug").open_issues_asof.max().nlargest(6).index
    st.line_chart(asof[asof.service_slug.isin(top)].pivot(index="asof_ts", columns="service_slug",
                                                          values="open_issues_asof"))

st.subheader("Backlog awaiting a first response")
st.dataframe(q("SELECT * FROM marts.backlog_by_service ORDER BY open_awaiting_first_response DESC LIMIT 20"),
             width="stretch")
