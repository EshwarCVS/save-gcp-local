"""AWS S3 connector.

Pulls files from ``s3://bucket/key`` to a local destination with optional
sampling.  Requires ``boto3``.
"""

from __future__ import annotations

import logging
import os

from . import Connector, _sample_dataframe, _write_auto

log = logging.getLogger("save_gcp_local.connectors.s3")

_TABULAR_EXTENSIONS = (".csv", ".parquet", ".json", ".jsonl", ".tsv")


def _parse_s3_uri(uri: str):
    for prefix in ("s3a://", "s3://"):
        if uri.startswith(prefix):
            path = uri[len(prefix):]
            parts = path.split("/", 1)
            return parts[0], parts[1] if len(parts) > 1 else ""
    raise ValueError(f"Not an S3 URI: {uri}")


class S3Connector(Connector):
    name = "s3"
    schemes = ("s3://", "s3a://")

    def pull(self, source: str, dest: str, sample_size: float = 1.0,
             seed: int = 42, **opts) -> str:
        import boto3

        bucket_name, key = _parse_s3_uri(source)
        s3 = boto3.client("s3")

        os.makedirs(os.path.dirname(dest) or ".", exist_ok=True)

        if key.endswith("/") or not key:
            return self._pull_prefix(s3, bucket_name, key, dest, sample_size, seed)

        _, ext = os.path.splitext(key)
        if ext.lower() in _TABULAR_EXTENSIONS and sample_size < 1.0:
            return self._pull_tabular(s3, bucket_name, key, dest, sample_size, seed)

        s3.download_file(bucket_name, key, dest)
        log.info("[save-gcp-local] S3: downloaded s3://%s/%s -> %s", bucket_name, key, dest)
        return dest

    def _pull_tabular(self, s3, bucket, key, dest, sample_size, seed):
        import io
        import pandas as pd

        obj = s3.get_object(Bucket=bucket, Key=key)
        data = obj["Body"].read()
        _, ext = os.path.splitext(key)

        if ext.lower() == ".parquet":
            df = pd.read_parquet(io.BytesIO(data))
        elif ext.lower() in (".json", ".jsonl"):
            df = pd.read_json(io.BytesIO(data), lines=True)
        elif ext.lower() == ".tsv":
            df = pd.read_csv(io.BytesIO(data), sep="\t")
        else:
            df = pd.read_csv(io.BytesIO(data))

        before = len(df)
        df = _sample_dataframe(df, sample_size, seed)
        _write_auto(df, dest)
        log.info(
            "[save-gcp-local] S3: pulled s3://%s/%s -> %s (%d/%d rows, %.0f%% sample)",
            bucket, key, dest, len(df), before, sample_size * 100,
        )
        return dest

    def _pull_prefix(self, s3, bucket, prefix, dest_dir, sample_size, seed):
        os.makedirs(dest_dir, exist_ok=True)
        paginator = s3.get_paginator("list_objects_v2")
        pulled = 0
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith("/"):
                    continue
                rel = key[len(prefix):].lstrip("/")
                local_path = os.path.join(dest_dir, rel)
                os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)

                _, ext = os.path.splitext(key)
                if ext.lower() in _TABULAR_EXTENSIONS and sample_size < 1.0:
                    self._pull_tabular(s3, bucket, key, local_path, sample_size, seed)
                else:
                    s3.download_file(bucket, key, local_path)
                pulled += 1

        log.info(
            "[save-gcp-local] S3: pulled %d files from s3://%s/%s -> %s",
            pulled, bucket, prefix, dest_dir,
        )
        return dest_dir
