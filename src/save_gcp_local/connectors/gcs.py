"""Google Cloud Storage connector.

Pulls files from ``gs://bucket/path`` to a local destination with optional
sampling.  Requires ``google-cloud-storage``.

For tabular files (CSV, Parquet, JSON), sampling reads the file into a
DataFrame, samples rows, and writes the result.  For binary or unknown
formats, the file is downloaded verbatim (sample_size is ignored).
"""

from __future__ import annotations

import logging
import os

from . import Connector, _sample_dataframe, _write_auto

log = logging.getLogger("save_gcp_local.connectors.gcs")

_TABULAR_EXTENSIONS = (".csv", ".parquet", ".json", ".jsonl", ".tsv")


def _parse_gs_uri(uri: str):
    """Parse gs://bucket/key into (bucket, key)."""
    path = uri[len("gs://"):]
    parts = path.split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""
    return bucket, key


class GCSConnector(Connector):
    name = "gcs"
    schemes = ("gs://",)

    def pull(self, source: str, dest: str, sample_size: float = 1.0,
             seed: int = 42, **opts) -> str:
        from google.cloud import storage as gcs_storage

        bucket_name, key = _parse_gs_uri(source)
        client = gcs_storage.Client()
        bucket = client.bucket(bucket_name)

        os.makedirs(os.path.dirname(dest) or ".", exist_ok=True)

        if key.endswith("/") or not key:
            return self._pull_prefix(bucket, key, dest, sample_size, seed, **opts)

        _, ext = os.path.splitext(key)
        if ext.lower() in _TABULAR_EXTENSIONS and sample_size < 1.0:
            return self._pull_tabular(bucket, key, dest, sample_size, seed)

        blob = bucket.blob(key)
        blob.download_to_filename(dest)
        log.info("[save-gcp-local] GCS: downloaded gs://%s/%s -> %s", bucket_name, key, dest)
        return dest

    def _pull_tabular(self, bucket, key, dest, sample_size, seed):
        import io
        import pandas as pd

        blob = bucket.blob(key)
        data = blob.download_as_bytes()
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
            "[save-gcp-local] GCS: pulled gs://%s/%s -> %s (%d/%d rows, %.0f%% sample)",
            bucket.name, key, dest, len(df), before, sample_size * 100,
        )
        return dest

    def _pull_prefix(self, bucket, prefix, dest_dir, sample_size, seed, **opts):
        """Pull all files under a GCS prefix into a local directory."""
        os.makedirs(dest_dir, exist_ok=True)
        blobs = list(bucket.list_blobs(prefix=prefix))
        pulled = 0
        for blob in blobs:
            if blob.name.endswith("/"):
                continue
            rel = blob.name[len(prefix):].lstrip("/")
            local_path = os.path.join(dest_dir, rel)
            os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)

            _, ext = os.path.splitext(blob.name)
            if ext.lower() in _TABULAR_EXTENSIONS and sample_size < 1.0:
                self._pull_tabular(bucket, blob.name, local_path, sample_size, seed)
            else:
                blob.download_to_filename(local_path)
            pulled += 1

        log.info(
            "[save-gcp-local] GCS: pulled %d files from gs://%s/%s -> %s",
            pulled, bucket.name, prefix, dest_dir,
        )
        return dest_dir
