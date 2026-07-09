"""Airflow connection patching for local development.

When running locally, DAGs reference Airflow connections that point to cloud
services (``google_cloud_default``, database connections, etc.).  This module
creates or overrides those connections at Airflow startup so they point to
local services instead — no manual Connection editing in the Airflow UI.

Works by either:
  1. Setting AIRFLOW_CONN_* environment variables (no DB required), or
  2. Programmatically creating Connection objects via the Airflow API.

The connection overrides are defined in a JSON config or via DPL_CONNECTIONS_*
environment variables.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Dict, List, Optional

log = logging.getLogger("save_gcp_local.connections")

# Default local connection overrides.  These replace cloud connections with
# local equivalents so DAGs parse and run without real credentials.
_DEFAULT_OVERRIDES: Dict[str, dict] = {
    "google_cloud_default": {
        "conn_type": "google_cloud_platform",
        "extra": json.dumps({
            "project": os.environ.get("DPL_GCP_PROJECT", "local-dev-project"),
            "num_retries": 0,
        }),
        "description": "[save-gcp-local] Mock GCP connection for local development",
    },
    "google_cloud_dataproc_default": {
        "conn_type": "google_cloud_platform",
        "extra": json.dumps({
            "project": os.environ.get("DPL_GCP_PROJECT", "local-dev-project"),
        }),
        "description": "[save-gcp-local] Mock Dataproc connection — operators are patched",
    },
    "hive_default": {
        "conn_type": "hiveserver2",
        "host": "local-hive",
        "port": 9083,
        "login": "hive",
        "schema": "default",
        "description": "[save-gcp-local] Local Hive metastore for dev/test",
    },
    "opensearch_default": {
        "conn_type": "http",
        "host": "local-opensearch",
        "port": 9200,
        "description": "[save-gcp-local] Local OpenSearch for dev/test",
    },
    "mssql_default": {
        "conn_type": "mssql",
        "host": "local-mssql",
        "port": 1433,
        "login": "sa",
        "password": "LocalPass#123",
        "schema": "master",
        "description": "[save-gcp-local] Local SQL Server (Azure DB equivalent) for dev/test",
    },
}


def _conn_env_key(conn_id: str) -> str:
    """AIRFLOW_CONN_{CONN_ID} — Airflow reads these as connection URIs."""
    return f"AIRFLOW_CONN_{conn_id.upper()}"


def build_connection_uri(spec: dict) -> str:
    """Build an Airflow connection URI from a spec dict.

    Format: conn_type://login:password@host:port/schema?extra_key=val
    """
    conn_type = spec.get("conn_type", "generic")
    login = spec.get("login", "")
    password = spec.get("password", "")
    host = spec.get("host", "")
    port = spec.get("port", "")
    schema = spec.get("schema", "")
    extra = spec.get("extra", "")

    auth = ""
    if login:
        auth = login
        if password:
            from urllib.parse import quote_plus
            auth += f":{quote_plus(password)}"
        auth += "@"

    port_str = f":{port}" if port else ""
    schema_str = f"/{schema}" if schema else ""

    uri = f"{conn_type}://{auth}{host}{port_str}{schema_str}"
    if extra:
        from urllib.parse import quote_plus
        if isinstance(extra, dict):
            extra = json.dumps(extra)
        uri += f"?extra__generic__extra={quote_plus(extra)}"
    return uri


def apply_connection_overrides(
    overrides: Optional[Dict[str, dict]] = None,
    include_defaults: bool = True,
) -> int:
    """Set AIRFLOW_CONN_* env vars for local connections.

    Returns the number of connections overridden.
    """
    merged: Dict[str, dict] = {}
    if include_defaults:
        merged.update(_DEFAULT_OVERRIDES)
    if overrides:
        merged.update(overrides)

    # Also pick up DPL_CONN_* env vars as JSON specs.
    for key, val in os.environ.items():
        if key.startswith("DPL_CONN_") and val:
            conn_id = key[len("DPL_CONN_"):].lower()
            try:
                merged[conn_id] = json.loads(val)
            except json.JSONDecodeError:
                merged[conn_id] = {"conn_type": "generic", "host": val}

    count = 0
    for conn_id, spec in merged.items():
        env_key = _conn_env_key(conn_id)
        if env_key in os.environ and not os.environ.get("DPL_FORCE_CONNECTIONS"):
            log.debug(
                "[save-gcp-local] %s already set, skipping (set DPL_FORCE_CONNECTIONS=true to override)",
                env_key,
            )
            continue
        uri = build_connection_uri(spec)
        os.environ[env_key] = uri
        log.info("[save-gcp-local] Connection override: %s -> local", conn_id)
        count += 1

    return count


def load_connections_file(path: str) -> Dict[str, dict]:
    """Load connection overrides from a JSON file.

    Format:
      {
        "conn_id": {
          "conn_type": "postgres",
          "host": "localhost",
          "port": 5432,
          "login": "user",
          "password": "pass",
          "schema": "mydb"
        }
      }
    """
    with open(path) as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Connections file must be a JSON object, got {type(data).__name__}")
    return data


def setup_local_connections(connections_file: Optional[str] = None) -> int:
    """One-call setup: load file if given, apply defaults + overrides.

    Called by the Airflow plugin on startup.
    """
    overrides = None
    filepath = connections_file or os.environ.get("DPL_CONNECTIONS_FILE")
    if filepath and os.path.isfile(filepath):
        try:
            overrides = load_connections_file(filepath)
            log.info("[save-gcp-local] Loaded %d connection overrides from %s", len(overrides), filepath)
        except Exception as e:
            log.warning("[save-gcp-local] Failed to load connections file %s: %s", filepath, e)

    return apply_connection_overrides(overrides)
