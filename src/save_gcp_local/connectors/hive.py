"""Hive connector.

Pulls data from Hive tables via PyHive or SQLAlchemy with a Hive dialect.
URI format: ``hive://host:port/database/table``

Requires ``pyhive`` and ``thrift`` (or ``sqlalchemy`` with a Hive driver).
"""

from __future__ import annotations

import logging
import os

from . import Connector, _sample_dataframe, _write_auto

log = logging.getLogger("save_gcp_local.connectors.hive")


def _parse_hive_uri(uri: str):
    """Parse hive://host:port/database/table -> (host, port, database, table)."""
    rest = uri[len("hive://"):]
    parts = rest.split("/", 2)
    host_port = parts[0] if parts else "localhost:10000"
    database = parts[1] if len(parts) > 1 else "default"
    table = parts[2] if len(parts) > 2 else ""

    if ":" in host_port:
        host, port_str = host_port.rsplit(":", 1)
        port = int(port_str)
    else:
        host = host_port
        port = 10000

    return host, port, database, table


class HiveConnector(Connector):
    name = "hive"
    schemes = ("hive://",)

    def pull(self, source: str, dest: str, sample_size: float = 1.0,
             seed: int = 42, **opts) -> str:
        host, port, database, table = _parse_hive_uri(source)

        table = opts.get("table", table)
        query = opts.get("query")
        if not table and not query:
            raise ValueError(
                "Hive connector requires a table name in the URI "
                "(hive://host:port/database/table) or --query"
            )

        os.makedirs(os.path.dirname(dest) or ".", exist_ok=True)

        try:
            return self._pull_pyhive(host, port, database, table, query,
                                     dest, sample_size, seed)
        except ImportError:
            return self._pull_sqlalchemy(host, port, database, table, query,
                                         dest, sample_size, seed)

    def _pull_pyhive(self, host, port, database, table, query,
                     dest, sample_size, seed):
        import pandas as pd
        from pyhive import hive

        conn = hive.connect(host=host, port=port, database=database)
        if query:
            sql = query
        elif sample_size < 1.0:
            sql = f"SELECT * FROM {table} TABLESAMPLE (BUCKET 1 OUT OF {max(int(1 / sample_size), 2)} ON rand())"
        else:
            sql = f"SELECT * FROM {table}"

        df = pd.read_sql(sql, conn)
        conn.close()

        if sample_size < 1.0 and not query:
            df = _sample_dataframe(df, sample_size, seed)

        before_note = f"(sampled to {len(df)} rows at {sample_size:.0%})" if sample_size < 1.0 else f"({len(df)} rows)"
        _write_auto(df, dest)
        log.info("[save-gcp-local] Hive: %s.%s -> %s %s", database, table, dest, before_note)
        return dest

    def _pull_sqlalchemy(self, host, port, database, table, query,
                         dest, sample_size, seed):
        import pandas as pd
        import sqlalchemy

        url = f"hive://{host}:{port}/{database}"
        engine = sqlalchemy.create_engine(url)

        if query:
            sql = query
        else:
            sql = f"SELECT * FROM {table}"

        df = pd.read_sql(sql, engine)
        if sample_size < 1.0:
            df = _sample_dataframe(df, sample_size, seed)

        _write_auto(df, dest)
        log.info("[save-gcp-local] Hive: %s.%s -> %s (%d rows)", database, table, dest, len(df))
        return dest
