"""Tests for the connectors module and pull-data CLI."""

import json
import os

import pytest


def _has_parquet_engine():
    try:
        import pyarrow  # noqa: F401
        return True
    except ImportError:
        pass
    try:
        import fastparquet  # noqa: F401
        return True
    except ImportError:
        return False


# ---------------------------------------------------------------- registry
def test_connector_registry_base():
    from save_gcp_local.connectors import Connector, register, available, get_connector

    @register
    class TestConnector(Connector):
        name = "test_dummy"
        schemes = ("test://",)
        def pull(self, source, dest, sample_size=1.0, seed=42, **opts):
            with open(dest, "w") as f:
                f.write(f"pulled:{source}:sample={sample_size}")
            return dest

    assert "test_dummy" in available()
    c = get_connector("test_dummy")
    assert c.name == "test_dummy"


def test_resolve_connector():
    from save_gcp_local.connectors import resolve_connector, register, Connector

    @register
    class TestResolve(Connector):
        name = "test_resolve"
        schemes = ("testresolve://",)
        def pull(self, source, dest, sample_size=1.0, seed=42, **opts):
            return dest

    c = resolve_connector("testresolve://bucket/key")
    assert c is not None
    assert c.name == "test_resolve"

    assert resolve_connector("unknown://something") is None


def test_get_connector_unknown():
    from save_gcp_local.connectors import get_connector
    with pytest.raises(KeyError, match="Unknown connector"):
        get_connector("nonexistent_xyz")


# --------------------------------------------------------- sampling helpers
def test_sample_dataframe():
    import pandas as pd
    from save_gcp_local.connectors import _sample_dataframe

    df = pd.DataFrame({"a": range(1000)})

    sampled = _sample_dataframe(df, 0.1, seed=42)
    assert 50 < len(sampled) < 200

    full = _sample_dataframe(df, 1.0)
    assert len(full) == 1000

    empty = _sample_dataframe(df, 0.0)
    assert len(empty) == 0


def test_write_auto_csv(tmp_path):
    import pandas as pd
    from save_gcp_local.connectors import _write_auto

    df = pd.DataFrame({"x": [1, 2, 3]})
    dest = str(tmp_path / "out.csv")
    _write_auto(df, dest)
    result = pd.read_csv(dest)
    assert len(result) == 3


@pytest.mark.skipif(
    not _has_parquet_engine(),
    reason="pyarrow or fastparquet not installed",
)
def test_write_auto_parquet(tmp_path):
    import pandas as pd
    from save_gcp_local.connectors import _write_auto

    df = pd.DataFrame({"x": [1, 2, 3]})
    dest = str(tmp_path / "out.parquet")
    _write_auto(df, dest)
    result = pd.read_parquet(dest)
    assert len(result) == 3


def test_write_auto_json(tmp_path):
    import pandas as pd
    from save_gcp_local.connectors import _write_auto

    df = pd.DataFrame({"x": [1, 2, 3]})
    dest = str(tmp_path / "out.json")
    _write_auto(df, dest)
    result = pd.read_json(dest, lines=True)
    assert len(result) == 3


# -------------------------------------------------------- GCS URI parsing
def test_gcs_parse_uri():
    from save_gcp_local.connectors.gcs import _parse_gs_uri

    bucket, key = _parse_gs_uri("gs://my-bucket/path/to/file.csv")
    assert bucket == "my-bucket"
    assert key == "path/to/file.csv"

    bucket, key = _parse_gs_uri("gs://bucket/")
    assert bucket == "bucket"
    assert key == ""

    bucket, key = _parse_gs_uri("gs://bucket")
    assert bucket == "bucket"
    assert key == ""


# --------------------------------------------------------- S3 URI parsing
def test_s3_parse_uri():
    from save_gcp_local.connectors.s3 import _parse_s3_uri

    bucket, key = _parse_s3_uri("s3://my-bucket/path/file.csv")
    assert bucket == "my-bucket"
    assert key == "path/file.csv"

    bucket, key = _parse_s3_uri("s3a://bucket/key")
    assert bucket == "bucket"
    assert key == "key"


def test_s3_parse_invalid():
    from save_gcp_local.connectors.s3 import _parse_s3_uri
    with pytest.raises(ValueError, match="Not an S3 URI"):
        _parse_s3_uri("gs://not-s3/key")


# ------------------------------------------------------- Azure URI parsing
def test_azure_parse_uri_abfss():
    from save_gcp_local.connectors.azure_blob import _parse_azure_uri

    url, container, path = _parse_azure_uri(
        "abfss://mycontainer@myaccount.dfs.core.windows.net/data/file.csv"
    )
    assert container == "mycontainer"
    assert "myaccount" in url
    assert path == "data/file.csv"


def test_azure_parse_uri_abfs():
    from save_gcp_local.connectors.azure_blob import _parse_azure_uri

    url, container, path = _parse_azure_uri(
        "abfs://container@account.blob.core.windows.net/path/to/data"
    )
    assert container == "container"
    assert path == "path/to/data"


def test_azure_parse_uri_wasbs():
    from save_gcp_local.connectors.azure_blob import _parse_azure_uri

    url, container, path = _parse_azure_uri(
        "wasbs://container@account.blob.core.windows.net/folder/"
    )
    assert container == "container"
    assert path == "folder/"


# -------------------------------------------------------- Hive URI parsing
def test_hive_parse_uri():
    from save_gcp_local.connectors.hive import _parse_hive_uri

    host, port, db, table = _parse_hive_uri("hive://myhost:10000/analytics/events")
    assert host == "myhost"
    assert port == 10000
    assert db == "analytics"
    assert table == "events"


def test_hive_parse_uri_defaults():
    from save_gcp_local.connectors.hive import _parse_hive_uri

    host, port, db, table = _parse_hive_uri("hive://localhost")
    assert host == "localhost"
    assert port == 10000
    assert db == "default"
    assert table == ""


# ----------------------------------------------------------- JDBC URL norm
def test_jdbc_normalize_url():
    from save_gcp_local.connectors.jdbc import _normalize_jdbc_url

    assert _normalize_jdbc_url("jdbc:postgresql://host/db") == "postgresql://host/db"
    assert _normalize_jdbc_url("postgresql://host/db") == "postgresql://host/db"


# ----------------------------------------------------------- pull config
def test_load_pull_config_json(tmp_path):
    config_data = {
        "default_sample_size": 0.2,
        "seed": 99,
        "sources": {
            "events": {
                "uri": "gs://bucket/events.csv",
                "dest": "./data/events.csv",
                "sample_size": 0.5,
            },
            "users": {
                "uri": "postgresql://host/db",
                "table": "users",
                "dest": "./data/users.parquet",
            },
        },
    }
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps(config_data))

    from save_gcp_local.pull_config import load_pull_config

    cfg = load_pull_config(str(config_file))
    assert cfg.default_sample_size == 0.2
    assert cfg.seed == 99
    assert len(cfg.sources) == 2
    assert cfg.sources[0].name == "events"
    assert cfg.sources[0].sample_size == 0.5
    assert cfg.sources[1].name == "users"
    assert cfg.sources[1].table == "users"
    assert cfg.sources[1].sample_size is None  # inherits default


# --------------------------------------------------------- CLI integration
def test_cli_connectors_command():
    from save_gcp_local.cli import main
    rc = main(["connectors"])
    assert rc == 0


def test_cli_pull_data_no_source():
    from save_gcp_local.cli import main
    rc = main(["pull-data"])
    assert rc == 2


def test_cli_pull_data_unknown_scheme():
    from save_gcp_local.cli import main
    rc = main(["pull-data", "--source", "ftp://unknown/path"])
    assert rc == 2


def test_cli_default_dest():
    from save_gcp_local.cli import _default_dest

    assert _default_dest("gs://bucket/events/2024.csv") == os.path.join("./data", "events/2024.csv")
    assert _default_dest("s3://bucket/raw/data.parquet") == os.path.join("./data", "raw/data.parquet")


def test_cli_pull_data_with_custom_connector(tmp_path):
    from save_gcp_local.connectors import register, Connector

    @register
    class FileTestConnector(Connector):
        name = "filetest"
        schemes = ("filetest://",)
        def pull(self, source, dest, sample_size=1.0, seed=42, **opts):
            os.makedirs(os.path.dirname(dest) or ".", exist_ok=True)
            with open(dest, "w") as f:
                f.write(f"data:sample={sample_size}")
            return dest

    dest = str(tmp_path / "output.csv")
    from save_gcp_local.cli import main
    rc = main(["pull-data", "--source", "filetest://bucket/data.csv",
               "--dest", dest, "--sample-size", "0.3"])
    assert rc == 0
    assert os.path.exists(dest)
    with open(dest) as f:
        assert "sample=0.3" in f.read()
