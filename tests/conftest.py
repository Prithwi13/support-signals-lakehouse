from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tests"))
os.environ.setdefault("SPARK_SHUFFLE_PARTITIONS", "2")


@pytest.fixture(scope="session")
def spark():
    from signals.transform.spark import get_spark

    s = get_spark("tests")
    yield s
    s.stop()


@pytest.fixture
def cfg(tmp_path, monkeypatch):
    monkeypatch.setenv("SIGNALS_STORAGE_ROOT", str(tmp_path / "data"))
    monkeypatch.setenv("SIGNALS_STATE_ROOT", str(tmp_path / "data/_state"))
    monkeypatch.setenv("SIGNALS_REPOS", "aws/aws-cdk,aws/aws-cli")
    monkeypatch.setenv("SIGNALS_MAX_PAGES", "100")
    monkeypatch.setenv("SIGNALS_WAREHOUSE", str(tmp_path / "wh.duckdb"))
    from signals.config import load_config

    c = load_config()
    c.github["per_page"] = 25
    return c
