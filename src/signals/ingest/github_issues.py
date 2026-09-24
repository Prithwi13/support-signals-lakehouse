"""Incremental extractor: GitHub issues + issue comments -> bronze (raw JSONL.gz).

Design notes
------------
* Incremental by watermark: each (repo, entity) keeps the max `updated_at` seen.
  The next run asks GitHub for `since = watermark - lookback`, sorted by updated
  ascending. Ascending order means a capped/interrupted run still advances the
  watermark safely: everything older than the last record seen was already read.
* The lookback window re-reads a small overlap to catch records whose
  `updated_at` was committed out of order. Duplicates are expected and removed
  in the silver layer (idempotent by design).
* The watermark is only saved *after* the bronze file is durably written, so a
  crash never skips data (at-least-once delivery).
* Bronze is immutable and raw: the full API payload is kept, wrapped with
  lineage columns (_run_id, _ingested_at, _source).
"""
from __future__ import annotations

import logging
import os
import re
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

from signals import storage
from signals.config import Config

log = logging.getLogger(__name__)

API = "https://api.github.com"
ENTITIES = {
    # entity name -> (endpoint template, extra params)
    "issues": ("/repos/{repo}/issues", {"state": "all"}),
    "issue_comments": ("/repos/{repo}/issues/comments", {}),
}
_NEXT_RE = re.compile(r'<([^>]+)>;\s*rel="next"')


class RateLimited(Exception):
    def __init__(self, reset_epoch: int):
        super().__init__(f"GitHub rate limit hit; resets at {reset_epoch}")
        self.reset_epoch = reset_epoch


@dataclass
class EntityResult:
    repo: str
    entity: str
    since: str
    records: int = 0
    pages: int = 0
    new_watermark: str | None = None
    stopped_reason: str = "complete"
    output_uri: str | None = None


@dataclass
class IngestReport:
    run_id: str
    started_at: str
    results: list[EntityResult] = field(default_factory=list)
    rate_limit_remaining: int | None = None
    duration_s: float = 0.0


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse(ts: str) -> datetime:
    return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


class GitHubClient:
    def __init__(self, token: str | None, timeout: int = 30, max_retries: int = 5,
                 session: requests.Session | None = None, sleep: Callable[[float], None] = time.sleep):
        self.s = session or requests.Session()
        self.s.headers.update({"Accept": "application/vnd.github+json",
                               "X-GitHub-Api-Version": "2022-11-28",
                               "User-Agent": "support-signals-lakehouse"})
        if token:
            self.s.headers["Authorization"] = f"Bearer {token}"
        self.timeout, self.max_retries, self.sleep = timeout, max_retries, sleep
        self.rate_remaining: int | None = None

    def get(self, url: str, params: dict | None = None) -> tuple[list[dict], str | None]:
        """GET one page. Retries 5xx/network errors with exponential backoff."""
        for attempt in range(self.max_retries + 1):
            try:
                r = self.s.get(url, params=params, timeout=self.timeout)
            except requests.RequestException as e:
                if attempt == self.max_retries:
                    raise
                log.warning("network error %s, retry %d", e, attempt + 1)
                self.sleep(2 ** attempt)
                continue
            if "X-RateLimit-Remaining" in r.headers:
                self.rate_remaining = int(r.headers["X-RateLimit-Remaining"])
            if r.status_code in (403, 429) and self.rate_remaining == 0:
                raise RateLimited(int(r.headers.get("X-RateLimit-Reset", "0")))
            if r.status_code >= 500 or r.status_code == 429:
                if attempt == self.max_retries:
                    r.raise_for_status()
                self.sleep(2 ** attempt)
                continue
            r.raise_for_status()
            m = _NEXT_RE.search(r.headers.get("Link", ""))
            return r.json(), (m.group(1) if m else None)
        raise RuntimeError("unreachable")


def _watermark_uri(cfg: Config, entity: str, repo: str) -> str:
    return f"{cfg.state_root.rstrip('/')}/watermarks/{entity}/{repo.replace('/', '__')}.json"


def extract_entity(cfg: Config, client: GitHubClient, repo: str, entity: str,
                   run_id: str, now: datetime) -> EntityResult:
    gh = cfg.github
    wm_uri = _watermark_uri(cfg, entity, repo)
    state = storage.read_json(wm_uri, default={})
    if state.get("watermark"):
        since_dt = _parse(state["watermark"]) - timedelta(minutes=gh.get("watermark_lookback_minutes", 60))
    else:
        since_dt = _parse(gh["initial_since"])
    res = EntityResult(repo=repo, entity=entity, since=_iso(since_dt))

    path, extra = ENTITIES[entity]
    url: str | None = API + path.format(repo=repo)
    params: dict | None = {"since": res.since, "sort": "updated", "direction": "asc",
                           "per_page": gh.get("per_page", 100), **extra}
    records: list[dict[str, Any]] = []
    max_seen: str | None = state.get("watermark")
    ingested_at = _iso(now)

    while url:
        if res.pages >= gh.get("max_pages_per_run", 10):
            res.stopped_reason = "page_cap"
            break
        try:
            page, url = client.get(url, params)
        except RateLimited as e:
            res.stopped_reason = f"rate_limited(reset={e.reset_epoch})"
            break
        params = None  # the "next" link already carries the query string
        res.pages += 1
        for item in page:
            records.append({"_run_id": run_id, "_ingested_at": ingested_at,
                            "_source": f"github:{repo}:{entity}", "repo": repo, "payload": item})
            ts = item.get("updated_at")
            if ts and (max_seen is None or ts > max_seen):
                max_seen = ts

    if records:
        day = now.strftime("%Y-%m-%d")
        res.output_uri = (f"{cfg.bronze}/{entity}/repo={repo.replace('/', '__')}/"
                          f"ingest_date={day}/{run_id}.jsonl.gz")
        res.records = storage.write_jsonl_gz(res.output_uri, records)
    # Only move the watermark after the data is safely written.
    if max_seen and max_seen != state.get("watermark"):
        storage.write_json(wm_uri, {"watermark": max_seen, "updated_by_run": run_id})
    res.new_watermark = max_seen
    log.info("%s %s: %d records, %d pages, stop=%s", repo, entity, res.records, res.pages, res.stopped_reason)
    return res


def run(cfg: Config, run_id: str, client: GitHubClient | None = None,
        now: datetime | None = None) -> IngestReport:
    t0 = time.time()
    now = now or datetime.now(timezone.utc)
    client = client or GitHubClient(os.environ.get("GITHUB_TOKEN"),
                                    timeout=cfg.github.get("request_timeout_s", 30),
                                    max_retries=cfg.github.get("max_retries", 5))
    report = IngestReport(run_id=run_id, started_at=_iso(now))
    for repo in cfg.github["repos"]:
        for entity in ENTITIES:
            report.results.append(extract_entity(cfg, client, repo, entity, run_id, now))
    report.rate_limit_remaining = client.rate_remaining
    report.duration_s = round(time.time() - t0, 2)
    storage.write_json(f"{cfg.state_root.rstrip('/')}/runs/{run_id}/ingest.json", report.__dict__ | {
        "results": [r.__dict__ for r in report.results]})
    return report
