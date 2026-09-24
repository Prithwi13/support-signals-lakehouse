"""Snapshot pointers (<table>/_current.json) without importing Spark.

Warehouse loaders only need to know which snapshot is current, so they import
from here and can run in lightweight environments (AWS CloudShell, Lambda, an
Airflow worker) that don't have PySpark installed.
"""
from __future__ import annotations

from signals import storage


def pointer_uri(table_uri: str) -> str:
    return f"{table_uri.rstrip('/')}/_current.json"


def current_version(table_uri: str) -> str | None:
    p = storage.read_json(pointer_uri(table_uri))
    return p["version"] if p else None
