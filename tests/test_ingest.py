from datetime import datetime, timedelta, timezone

from fake_github import START, FakeGitHub

from signals import storage
from signals.ingest import github_issues as gi


def client(fake):
    return gi.GitHubClient(None, session=fake, sleep=lambda s: None)


def test_first_run_reads_everything_and_sets_watermark(cfg):
    fake = FakeGitHub().seed(["aws/aws-cdk", "aws/aws-cli"], 60, START)
    rep = gi.run(cfg, "r1", client(fake), now=datetime(2025, 6, 1, tzinfo=timezone.utc))
    issues = {r.repo: r for r in rep.results if r.entity == "issues"}
    assert issues["aws/aws-cdk"].records == 60           # PRs are kept raw in bronze
    assert issues["aws/aws-cdk"].stopped_reason == "complete"
    wm = storage.read_json(gi._watermark_uri(cfg, "issues", "aws/aws-cdk"))
    assert wm["watermark"] == max(i["updated_at"] for i in fake.issues["aws/aws-cdk"])


def test_second_run_is_incremental(cfg):
    fake = FakeGitHub().seed(["aws/aws-cdk", "aws/aws-cli"], 60, START)
    now = datetime(2025, 6, 1, tzinfo=timezone.utc)
    gi.run(cfg, "r1", client(fake), now=now)
    fake.touch("aws/aws-cdk", 5, now + timedelta(days=1), state="closed")
    rep = gi.run(cfg, "r2", client(fake), now=now + timedelta(days=1))
    cdk = next(r for r in rep.results if r.repo == "aws/aws-cdk" and r.entity == "issues")
    # only the changed issue (+ anything inside the 60-minute lookback overlap)
    assert 1 <= cdk.records <= 3


def test_page_cap_advances_watermark_safely(cfg):
    fake = FakeGitHub().seed(["aws/aws-cdk", "aws/aws-cli"], 100, START)
    cfg.github["max_pages_per_run"] = 2
    rep = gi.run(cfg, "r1", client(fake), now=datetime(2025, 6, 1, tzinfo=timezone.utc))
    cdk = next(r for r in rep.results if r.repo == "aws/aws-cdk" and r.entity == "issues")
    assert cdk.stopped_reason == "page_cap" and cdk.records == 50
    # resume: keep running until complete, we must end with every issue captured
    seen = set()
    for i in range(10):
        r = gi.run(cfg, f"r{i + 2}", client(fake), now=datetime(2025, 6, 1, tzinfo=timezone.utc))
        if all(x.stopped_reason == "complete" for x in r.results):
            break
    import glob
    import gzip
    import json
    for f in glob.glob(f"{cfg.bronze}/issues/repo=aws__aws-cdk/**/*.jsonl.gz", recursive=True):
        for line in gzip.open(f, "rt"):
            seen.add(json.loads(line)["payload"]["id"])
    assert seen == {i["id"] for i in fake.issues["aws/aws-cdk"]}


def test_rate_limit_stops_gracefully(cfg):
    fake = FakeGitHub().seed(["aws/aws-cdk", "aws/aws-cli"], 100, START)
    fake.rate_limit_after = 1
    rep = gi.run(cfg, "r1", client(fake), now=datetime(2025, 6, 1, tzinfo=timezone.utc))
    assert rep.results[0].records == 25
    assert rep.results[1].stopped_reason.startswith("rate_limited")
