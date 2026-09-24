"""In-memory fake of the two GitHub endpoints the extractor uses.

Honors `since`, sort=updated asc, per_page and Link-header pagination, and can
simulate rate limiting — enough to test incremental behaviour without network.
"""
from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlencode, urlparse


def iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


class Resp:
    def __init__(self, status: int, body, headers: dict):
        self.status_code, self._body, self.headers = status, body, headers

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeGitHub:
    def __init__(self):
        self.headers: dict = {}
        self.issues: dict[str, list[dict]] = {}
        self.comments: dict[str, list[dict]] = {}
        self.calls = 0
        self.rate_limit_after: int | None = None

    # ------------------------------------------------------------ data builders
    def seed(self, repos, n_issues: int, start: datetime, seed: int = 7):
        rnd = random.Random(seed)
        labels_pool = ["bug", "feature-request", "duplicate", "needs-triage", "p1", "guidance",
                       "@aws-cdk/aws-lambda", "@aws-cdk/aws-s3", "client-dynamodb", "service/ec2"]
        titles = ["S3 presigned url fails with 403", "Lambda timeout on cold start", "DynamoDB query pagination bug",
                  "EC2 describe instances throttling", "credentials not found with sso profile",
                  "cloudformation stack stuck in UPDATE_ROLLBACK", "Feature: support new region",
                  "kinesis putrecords partial failure", "cdk synth crashes on nested stacks", "docs typo"]
        gid = 1000
        for repo in repos:
            self.issues[repo], self.comments[repo] = [], []
            for n in range(1, n_issues + 1):
                gid += 1
                created = start + timedelta(hours=rnd.randint(0, 24 * 120))
                author = f"user{rnd.randint(1, 50)}"
                closed = rnd.random() < 0.6
                closed_at = created + timedelta(hours=rnd.randint(1, 24 * 40)) if closed else None
                updated = max(closed_at or created, created + timedelta(hours=rnd.randint(0, 48)))
                is_pr = rnd.random() < 0.1
                issue = {
                    "id": gid, "number": n, "title": rnd.choice(titles) + f" #{n}", "body": "repro:\n```\ncode\n```",
                    "state": "closed" if closed else "open", "state_reason": "completed" if closed else None,
                    "user": {"login": author, "type": "User"}, "author_association": "NONE",
                    "labels": [{"name": x} for x in rnd.sample(labels_pool, rnd.randint(0, 3))],
                    "assignees": [], "milestone": None, "comments": 0, "reactions": {"total_count": rnd.randint(0, 9)},
                    "html_url": f"https://github.com/{repo}/issues/{n}",
                    "created_at": iso(created), "updated_at": iso(updated),
                    "closed_at": iso(closed_at) if closed_at else None,
                    "node_id": "x", "url": "u",
                }
                if is_pr:
                    issue["pull_request"] = {"url": "pr"}
                self.issues[repo].append(issue)
                for c in range(rnd.randint(0, 3)):
                    gid += 1
                    maint = rnd.random() < 0.5
                    self.comments[repo].append({
                        "id": gid, "issue_url": f"https://api.github.com/repos/{repo}/issues/{n}",
                        "body": "thanks", "user": {"login": "maintainer1" if maint else author, "type": "User"},
                        "author_association": "MEMBER" if maint else "NONE",
                        "created_at": iso(created + timedelta(hours=rnd.randint(1, 72) + c)),
                        "updated_at": iso(created + timedelta(hours=rnd.randint(1, 72) + c)),
                    })
                    issue["comments"] += 1
        return self

    # ------------------------------------------------------------ HTTP surface
    def get(self, url, params=None, timeout=None):
        self.calls += 1
        if self.rate_limit_after is not None and self.calls > self.rate_limit_after:
            return Resp(403, {"message": "rate limited"}, {"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "1"})
        u = urlparse(url)
        q = {k: v[0] for k, v in parse_qs(u.query).items()}
        q.update({k: str(v) for k, v in (params or {}).items()})
        parts = u.path.strip("/").split("/")
        repo = f"{parts[1]}/{parts[2]}"
        rows = self.comments[repo] if parts[-1] == "comments" else self.issues[repo]
        since = q.get("since")
        sel = sorted([r for r in rows if not since or r["updated_at"] >= since],
                     key=lambda r: (r["updated_at"], r["id"]))
        per, page = int(q.get("per_page", 30)), int(q.get("page", 1))
        chunk = sel[(page - 1) * per: page * per]
        headers = {"X-RateLimit-Remaining": "4999"}
        if page * per < len(sel):
            nq = dict(q, page=str(page + 1))
            headers["Link"] = f'<{u.scheme}://{u.netloc}{u.path}?{urlencode(nq)}>; rel="next"'
        return Resp(200, chunk, headers)

    # ------------------------------------------------------------ mutations
    def touch(self, repo: str, number: int, when: datetime, **changes):
        for i in self.issues[repo]:
            if i["number"] == number:
                i.update(changes)
                i["updated_at"] = iso(when)
                return i
        raise KeyError(number)


START = datetime(2025, 1, 1, tzinfo=timezone.utc)
