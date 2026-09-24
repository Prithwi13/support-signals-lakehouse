"""Load pipeline config from YAML with environment-variable overrides."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = Path(os.environ.get("SIGNALS_CONFIG_DIR", REPO_ROOT / "config"))


def _load(name: str) -> dict[str, Any]:
    with open(CONFIG_DIR / name, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


@dataclass
class Config:
    storage_root: str
    state_root: str
    github: dict[str, Any]
    sla: dict[str, Any]
    quality: dict[str, Any]
    service_map: dict[str, Any] = field(default_factory=dict)
    quality_checks: dict[str, Any] = field(default_factory=dict)

    # ---- layer paths -------------------------------------------------
    def path(self, *parts: str) -> str:
        return "/".join([self.storage_root.rstrip("/"), *parts])

    @property
    def bronze(self) -> str:
        return self.path("bronze")

    @property
    def silver(self) -> str:
        return self.path("silver")

    @property
    def gold(self) -> str:
        return self.path("gold")

    @property
    def is_s3(self) -> bool:
        return self.storage_root.startswith("s3://")


def load_config() -> Config:
    raw = _load("pipeline.yaml")
    cfg = Config(
        storage_root=os.environ.get("SIGNALS_STORAGE_ROOT", raw["storage_root"]),
        state_root=os.environ.get("SIGNALS_STATE_ROOT", raw["state_root"]),
        github=raw["github"],
        sla=raw["sla"],
        quality=raw["quality"],
        service_map=_load("service_map.yaml"),
        quality_checks=_load("quality.yaml"),
    )
    if os.environ.get("SIGNALS_REPOS"):
        cfg.github["repos"] = [r.strip() for r in os.environ["SIGNALS_REPOS"].split(",") if r.strip()]
    if os.environ.get("SIGNALS_MAX_PAGES"):
        cfg.github["max_pages_per_run"] = int(os.environ["SIGNALS_MAX_PAGES"])
    if os.environ.get("SIGNALS_INITIAL_SINCE"):
        cfg.github["initial_since"] = os.environ["SIGNALS_INITIAL_SINCE"]
    return cfg
