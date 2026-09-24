"""Tiny storage abstraction so the same code writes to a local folder or to S3."""
from __future__ import annotations

import gzip
import json
import os
from collections.abc import Iterable
from typing import Any
from urllib.parse import urlparse


def _split_s3(uri: str) -> tuple[str, str]:
    p = urlparse(uri)
    return p.netloc, p.path.lstrip("/")


def _s3():
    import boto3  # imported lazily so local runs don't need AWS libs configured

    return boto3.client("s3")


def write_bytes(uri: str, data: bytes) -> None:
    if uri.startswith("s3://"):
        bucket, key = _split_s3(uri)
        _s3().put_object(Bucket=bucket, Key=key, Body=data)
        return
    os.makedirs(os.path.dirname(uri) or ".", exist_ok=True)
    tmp = uri + ".tmp"
    with open(tmp, "wb") as f:
        f.write(data)
    os.replace(tmp, uri)  # atomic on POSIX: readers never see a half-written file


def read_bytes(uri: str) -> bytes | None:
    if uri.startswith("s3://"):
        bucket, key = _split_s3(uri)
        try:
            return _s3().get_object(Bucket=bucket, Key=key)["Body"].read()
        except _s3().exceptions.NoSuchKey:
            return None
    if not os.path.exists(uri):
        return None
    with open(uri, "rb") as f:
        return f.read()


def write_jsonl_gz(uri: str, records: Iterable[dict[str, Any]]) -> int:
    lines = [json.dumps(r, separators=(",", ":"), default=str) for r in records]
    write_bytes(uri, gzip.compress(("\n".join(lines) + "\n").encode("utf-8")))
    return len(lines)


def read_json(uri: str, default: Any = None) -> Any:
    raw = read_bytes(uri)
    return default if raw is None else json.loads(raw)


def write_json(uri: str, obj: Any) -> None:
    write_bytes(uri, json.dumps(obj, indent=2, default=str).encode("utf-8"))
