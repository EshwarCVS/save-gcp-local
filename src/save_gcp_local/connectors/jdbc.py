"""Generic JDBC / SQL database connector.

Pulls data from any database supported by SQLAlchemy (PostgreSQL, MySQL,
Oracle, MSSQL, SQLite, etc.).  Requires ``sqlalchemy`` and the appropriate
database driver.

URI formats:
  - SQLAlchemy URLs: ``postgresql://user:pass@host:5432/db``
  - JDBC-style prefix: ``jdbc:postgresql://...`` (the ``jdbc:`` prefix is stripped)
"""

from __future__ import annotations

import logging
import os

from . import Connector, _sample_dataframe, _write_auto

log = logging.getLogger("save_gcp_local.connectors.jdbc")


def _normalize_jdbc_url(uri: str) -> str:
    if uri.startswith("jdbc:"):
        return uri[len("jdbc:"):]
    return uri


class JDBCConnector(Connector):
    name = "jdbc"
    schemes = ("jdbc:", "postgresql://", "mysql://", "oracle://", "mssql://", "sqlite:///")

    def pull(self, source: str, dest: str, sample_size: float = 1.0,
             seed: int = 42, **opts) -> str:
        import pandas as pd
        import sqlalchemy

        url = _normalize_jdbc_url(source)
        table = opts.get("table", "")
        query = opts.get("query", "")
        limit = opts.get("limit")

        if not table and not query:
            raise ValueError(
                "JDBC connector requires --table or --query. "
                "Example: save-gcp-local pull-data --source postgresql://host/db --table events"
            )

        engine = sqlalchemy.create_engine(url)
        os.makedirs(os.path.dirname(dest) or ".", exist_ok=True)

        if query:
            sql = query
        else:
            sql = f"SELECT * FROM {table}"
            if limit:
                sql += f" LIMIT {int(limit)}"

        df = pd.read_sql(sql, engine)
        before = len(df)

        if sample_size < 1.0:
            df = _sample_dataframe(df, sample_size, seed)

        _write_auto(df, dest)
        log.info(
            "[save-gcp-local] JDBC: %s -> %s (%d/%d rows, %.0f%% sample)",
            table or "query", dest, len(df), before, sample_size * 100,
        )
        return dest
