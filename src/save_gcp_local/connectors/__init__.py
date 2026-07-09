"""Pluggable data connectors for pulling sample data from remote sources.

Each connector knows how to read from a specific storage backend (GCS, S3,
Azure Blob, Hive, JDBC databases) and write a sampled subset to a local
destination.  Connectors are registered by URI scheme so ``pull-data`` can
auto-dispatch based on the source URI.

Built-in connectors
-------------------
  gcs       Google Cloud Storage (gs://)
  s3        AWS S3 (s3://, s3a://)
  azure     Azure Blob / ADLS (abfs://, abfss://, wasbs://)
  hive      Hive tables via PyHive JDBC (hive://)
  jdbc      Generic SQL databases via SQLAlchemy (jdbc:, postgresql://, mysql://, etc.)

Custom connectors
-----------------
Subclass ``Connector``, implement ``pull()``, and register:

    from save_gcp_local.connectors import register, Connector

    @register
    class MyConnector(Connector):
        name = "mystore"
        schemes = ("myproto://",)
        def pull(self, source, dest, sample_size, **opts): ...
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional, Type

log = logging.getLogger("save_gcp_local.connectors")


class Connector:
    """Base class for data connectors."""

    name: str = "base"
    schemes: tuple = ()

    def pull(
        self,
        source: str,
        dest: str,
        sample_size: float = 1.0,
        seed: int = 42,
        **opts,
    ) -> str:
        """Pull data from ``source`` to ``dest``, sampling ``sample_size`` fraction.

        Args:
            source:      URI or path identifying the remote data.
            dest:        Local file path to write to.
            sample_size: Fraction of data to keep (0.0-1.0).  1.0 = all data.
            seed:        Random seed for reproducible sampling.
            **opts:      Connector-specific options (format, query, etc.).

        Returns:
            The destination path written.
        """
        raise NotImplementedError


_REGISTRY: Dict[str, Type[Connector]] = {}
_SCHEME_MAP: Dict[str, str] = {}


def register(connector_cls: Type[Connector]) -> Type[Connector]:
    _REGISTRY[connector_cls.name] = connector_cls
    for scheme in connector_cls.schemes:
        _SCHEME_MAP[scheme] = connector_cls.name
    return connector_cls


def get_connector(name: str) -> Connector:
    if name not in _REGISTRY:
        raise KeyError(
            f"Unknown connector '{name}'. Available: {sorted(_REGISTRY)}"
        )
    return _REGISTRY[name]()


def resolve_connector(uri: str) -> Optional[Connector]:
    """Return the connector whose scheme matches ``uri``, or None."""
    for scheme, name in _SCHEME_MAP.items():
        if uri.startswith(scheme):
            return _REGISTRY[name]()
    return None


def available() -> List[str]:
    return sorted(_REGISTRY)


def available_detail() -> Dict[str, List[str]]:
    """Return {name: [schemes...]} for all registered connectors."""
    return {name: list(cls.schemes) for name, cls in sorted(_REGISTRY.items())}


def _sample_dataframe(df, sample_size: float, seed: int = 42):
    """Sample a pandas DataFrame by fraction."""
    if sample_size >= 1.0:
        return df
    if sample_size <= 0.0:
        return df.iloc[:0]
    return df.sample(frac=sample_size, random_state=seed)


def _write_auto(df, dest: str) -> str:
    """Write a DataFrame to dest, inferring format from extension."""
    import os
    os.makedirs(os.path.dirname(dest) or ".", exist_ok=True)
    if dest.endswith(".parquet"):
        df.to_parquet(dest, index=False)
    elif dest.endswith(".json") or dest.endswith(".jsonl"):
        df.to_json(dest, orient="records", lines=True)
    else:
        df.to_csv(dest, index=False)
    return dest


# Register built-in connectors.  Each import is guarded so missing
# dependencies don't break the core.
try:
    from .gcs import GCSConnector
    register(GCSConnector)
except Exception:
    pass

try:
    from .s3 import S3Connector
    register(S3Connector)
except Exception:
    pass

try:
    from .azure_blob import AzureBlobConnector
    register(AzureBlobConnector)
except Exception:
    pass

try:
    from .hive import HiveConnector
    register(HiveConnector)
except Exception:
    pass

try:
    from .jdbc import JDBCConnector
    register(JDBCConnector)
except Exception:
    pass
