"""Data-science layer built on gold: topic clustering + escalation-risk scoring.

This is the "partner with data scientists" piece: the pipeline supplies clean,
leak-free feature tables; models write their outputs back to gold so BI and the
warehouse can join them like any other table.

* issue_topics      TF-IDF on titles + MiniBatchKMeans -> topic_id + top terms
* escalation_scores logistic regression predicting "slow resolution" (> N days
                    to close) from information available when the issue is opened.
                    Train/test split is by time (train on older, test on newest)
                    so the evaluation mirrors how the model would be used.
"""
from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from signals.config import Config
from signals.transform.gold import enrich_issues
from signals.transform.spark import publish_table, read_table

log = logging.getLogger(__name__)


def _load(spark: SparkSession, cfg: Config) -> pd.DataFrame:
    issues = enrich_issues(read_table(spark, f"{cfg.silver}/issues"), cfg)
    fact = read_table(spark, f"{cfg.gold}/fact_issue")
    df = (issues.select("issue_id", "repo", "title", "service_slug", "author_association", "created_at")
          .join(fact.select("issue_id", "is_closed", "hours_to_close", "open_age_hours", "body_len",
                            "has_code_block"), "issue_id"))
    return df.withColumn("title", F.coalesce("title", F.lit(""))).toPandas()


def topics(pdf: pd.DataFrame, k: int) -> tuple[pd.DataFrame, dict[int, str]]:
    from sklearn.cluster import MiniBatchKMeans
    from sklearn.feature_extraction.text import TfidfVectorizer

    vec = TfidfVectorizer(max_features=5000, stop_words="english", ngram_range=(1, 2), min_df=3,
                          token_pattern=r"(?u)\b[a-zA-Z][a-zA-Z0-9_\-]{2,}\b")
    X = vec.fit_transform(pdf["title"])
    k = max(2, min(k, X.shape[0] // 20))
    km = MiniBatchKMeans(n_clusters=k, random_state=42, n_init=5, batch_size=1024).fit(X)
    terms = np.array(vec.get_feature_names_out())
    names = {i: ", ".join(terms[np.argsort(c)[::-1][:5]]) for i, c in enumerate(km.cluster_centers_)}
    out = pd.DataFrame({"issue_id": pdf["issue_id"], "topic_id": km.labels_.astype("int32")})
    out["topic_terms"] = out["topic_id"].map(names)
    return out, names


def escalation(pdf: pd.DataFrame, slow_days: int) -> tuple[pd.DataFrame, dict[str, Any]]:
    from sklearn.compose import ColumnTransformer
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import roc_auc_score
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OneHotEncoder, StandardScaler

    h = slow_days * 24
    d = pdf.copy()
    # Label is known only if closed, or open for longer than the threshold already.
    d["label"] = np.where(d["is_closed"], (d["hours_to_close"] > h).astype(float),
                          np.where(d["open_age_hours"] > h, 1.0, np.nan))
    d["log_body_len"] = np.log1p(d["body_len"].fillna(0))
    d["title_len"] = d["title"].str.len()
    d["has_code_block"] = d["has_code_block"].fillna(False).astype(int)
    feats = ["title", "repo", "service_slug", "author_association", "log_body_len", "title_len", "has_code_block"]
    labeled = d.dropna(subset=["label"]).sort_values("created_at")
    if len(labeled) < 200 or labeled["label"].nunique() < 2:
        return pd.DataFrame(columns=["issue_id", "escalation_risk"]), {"skipped": "not enough labeled data"}

    cut = int(len(labeled) * 0.8)
    train, test = labeled.iloc[:cut], labeled.iloc[cut:]
    pre = ColumnTransformer([
        ("title", TfidfVectorizer(max_features=3000, stop_words="english", min_df=2), "title"),
        ("cat", OneHotEncoder(handle_unknown="ignore"), ["repo", "service_slug", "author_association"]),
        ("num", StandardScaler(), ["log_body_len", "title_len", "has_code_block"]),
    ])
    model = Pipeline([("pre", pre), ("clf", LogisticRegression(max_iter=2000, class_weight="balanced"))])
    model.fit(train[feats], train["label"])
    p = model.predict_proba(test[feats])[:, 1]
    top = test.assign(p=p).nlargest(max(1, len(test) // 10), "p")
    metrics = {
        "train_rows": int(len(train)), "test_rows": int(len(test)),
        "test_period": f"{test['created_at'].min()} .. {test['created_at'].max()}",
        "base_rate": round(float(test["label"].mean()), 4),
        "roc_auc": round(float(roc_auc_score(test["label"], p)), 4) if test["label"].nunique() > 1 else None,
        "precision_at_top_10pct": round(float(top["label"].mean()), 4),
    }
    scores = pd.DataFrame({"issue_id": d["issue_id"], "escalation_risk": model.predict_proba(d[feats])[:, 1]})
    return scores, metrics


def run(spark: SparkSession, cfg: Config, run_id: str, k: int = 20) -> dict[str, Any]:
    pdf = _load(spark, cfg)
    rep: dict[str, Any] = {"rows": int(len(pdf))}
    if len(pdf) < 60:
        return rep | {"skipped": "not enough issues for modeling"}
    t, names = topics(pdf, k)
    rep["topics"] = {int(i): n for i, n in names.items()}
    rep["tables"] = {"issue_topics": publish_table(spark.createDataFrame(t), f"{cfg.gold}/issue_topics",
                                                   run_id, coalesce=1)}
    s, metrics = escalation(pdf, cfg.sla["slow_resolution_days"])
    rep["escalation_model"] = metrics
    if len(s):
        rep["tables"]["escalation_scores"] = publish_table(spark.createDataFrame(s), f"{cfg.gold}/escalation_scores",
                                                           run_id, coalesce=1)
    return rep
