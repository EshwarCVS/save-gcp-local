"""Azure Blob / ADLS connector.

Pulls files from ``abfs://``, ``abfss://``, or ``wasbs://`` URIs to a local
destination with optional sampling.  Requires ``azure-storage-blob``.
"""

from __future__ import annotations

import logging
import os

from . import Connector, _sample_dataframe, _write_auto

log = logging.getLogger("save_gcp_local.connectors.azure_blob")

_TABULAR_EXTENSIONS = (".csv", ".parquet", ".json", ".jsonl", ".tsv")


def _parse_azure_uri(uri: str):
    """Parse abfs[s]://container@account.dfs.core.windows.net/path or wasbs://container@account/path.

    Returns (account_url, container, blob_path).
    """
    for prefix in ("abfss://", "abfs://", "wasbs://"):
        if uri.startswith(prefix):
            rest = uri[len(prefix):]
            break
    else:
        raise ValueError(f"Not an Azure URI: {uri}")

    if "@" in rest.split("/")[0]:
        container_and_account, *path_parts = rest.split("/", 1)
        container, account_host = container_and_account.split("@", 1)
        blob_path = path_parts[0] if path_parts else ""
        if ".blob." not in account_host and ".dfs." not in account_host:
            account_host = account_host + ".blob.core.windows.net"
        account_url = f"https://{account_host}"
    else:
        parts = rest.split("/", 2)
        container = parts[0] if len(parts) > 0 else ""
        blob_path = parts[1] if len(parts) > 1 else ""
        account_url = ""

    return account_url, container, blob_path


class AzureBlobConnector(Connector):
    name = "azure"
    schemes = ("abfs://", "abfss://", "wasbs://")

    def pull(self, source: str, dest: str, sample_size: float = 1.0,
             seed: int = 42, **opts) -> str:
        from azure.storage.blob import BlobServiceClient
        from azure.identity import DefaultAzureCredential

        account_url, container_name, blob_path = _parse_azure_uri(source)

        connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
        if connection_string:
            client = BlobServiceClient.from_connection_string(connection_string)
        elif account_url:
            client = BlobServiceClient(account_url, credential=DefaultAzureCredential())
        else:
            raise ValueError(
                "Set AZURE_STORAGE_CONNECTION_STRING or provide a full "
                "abfs://container@account.dfs.core.windows.net/path URI"
            )

        container = client.get_container_client(container_name)
        os.makedirs(os.path.dirname(dest) or ".", exist_ok=True)

        if blob_path.endswith("/") or not blob_path:
            return self._pull_prefix(container, blob_path, dest, sample_size, seed)

        _, ext = os.path.splitext(blob_path)
        if ext.lower() in _TABULAR_EXTENSIONS and sample_size < 1.0:
            return self._pull_tabular(container, blob_path, dest, sample_size, seed)

        blob = container.get_blob_client(blob_path)
        with open(dest, "wb") as f:
            stream = blob.download_blob()
            stream.readinto(f)
        log.info("[save-gcp-local] Azure: downloaded %s -> %s", source, dest)
        return dest

    def _pull_tabular(self, container, blob_path, dest, sample_size, seed):
        import io
        import pandas as pd

        blob = container.get_blob_client(blob_path)
        data = blob.download_blob().readall()
        _, ext = os.path.splitext(blob_path)

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
            "[save-gcp-local] Azure: pulled %s -> %s (%d/%d rows, %.0f%% sample)",
            blob_path, dest, len(df), before, sample_size * 100,
        )
        return dest

    def _pull_prefix(self, container, prefix, dest_dir, sample_size, seed):
        os.makedirs(dest_dir, exist_ok=True)
        blobs = container.list_blobs(name_starts_with=prefix)
        pulled = 0
        for blob in blobs:
            if blob.name.endswith("/"):
                continue
            rel = blob.name[len(prefix):].lstrip("/")
            local_path = os.path.join(dest_dir, rel)
            os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)

            _, ext = os.path.splitext(blob.name)
            if ext.lower() in _TABULAR_EXTENSIONS and sample_size < 1.0:
                self._pull_tabular(container, blob.name, local_path, sample_size, seed)
            else:
                blob_client = container.get_blob_client(blob.name)
                with open(local_path, "wb") as f:
                    stream = blob_client.download_blob()
                    stream.readinto(f)
            pulled += 1

        log.info(
            "[save-gcp-local] Azure: pulled %d files from prefix '%s' -> %s",
            pulled, prefix, dest_dir,
        )
        return dest_dir
