"""Tests for Astro CLI scaffolding and connection patching."""

import json
import os

import pytest


# --------------------------------------------------------- astro scaffolding
def test_scaffold_creates_all_files(tmp_path):
    """init-astro generates the expected files in the project directory."""
    # Create a minimal Astro project skeleton
    (tmp_path / "Dockerfile").write_text("FROM quay.io/astronomer/astro-runtime:12.0.0\n")
    (tmp_path / "requirements.txt").write_text("apache-airflow-providers-google\n")
    (tmp_path / "plugins").mkdir()
    (tmp_path / "dags").mkdir()
    (tmp_path / "include").mkdir()

    from save_gcp_local.astro import scaffold_astro_project
    results = scaffold_astro_project(str(tmp_path))

    assert results["Dockerfile"] == "patched"
    assert results["plugins/save_gcp_local_plugin.py"] == "created"
    assert results[".env"] == "created"
    assert results["docker-compose.override.yml"] == "created"
    assert results["include/local_connections.json"] == "created"
    assert results["requirements.txt"] == "updated"

    # Verify Dockerfile was appended
    content = (tmp_path / "Dockerfile").read_text()
    assert "save-gcp-local" in content
    assert "SPARK_HOME" in content
    assert "openjdk" in content

    # Verify plugin file
    assert (tmp_path / "plugins" / "save_gcp_local_plugin.py").exists()
    plugin_content = (tmp_path / "plugins" / "save_gcp_local_plugin.py").read_text()
    assert "airflow_plugin" in plugin_content
    assert "setup_local_connections" in plugin_content

    # Verify .env
    env_content = (tmp_path / ".env").read_text()
    assert "DPL_ENABLED=true" in env_content
    assert "DPL_RUNNER=local" in env_content
    assert "DPL_SPARK_SUBMIT_CMD" in env_content

    # Verify docker-compose
    compose = (tmp_path / "docker-compose.override.yml").read_text()
    assert "local-postgres" in compose

    # Verify connections
    conn = json.loads((tmp_path / "include" / "local_connections.json").read_text())
    assert "google_cloud_default" in conn
    assert "local_postgres" in conn

    # Verify include dirs created
    assert (tmp_path / "include" / "jobs").is_dir()
    assert (tmp_path / "include" / "data").is_dir()
    assert (tmp_path / "include" / "output").is_dir()

    # Verify requirements.txt updated
    req = (tmp_path / "requirements.txt").read_text()
    assert "save-gcp-local" in req


def test_scaffold_idempotent(tmp_path):
    """Running init-astro twice doesn't duplicate content."""
    (tmp_path / "Dockerfile").write_text("FROM quay.io/astronomer/astro-runtime:12.0.0\n")
    (tmp_path / "plugins").mkdir()

    from save_gcp_local.astro import scaffold_astro_project

    # First run
    scaffold_astro_project(str(tmp_path))
    content_after_first = (tmp_path / "Dockerfile").read_text()

    # Second run
    results = scaffold_astro_project(str(tmp_path))
    content_after_second = (tmp_path / "Dockerfile").read_text()

    assert results["Dockerfile"] == "already patched"
    assert results["plugins/save_gcp_local_plugin.py"] == "exists"
    assert content_after_first == content_after_second


def test_scaffold_skip_compose(tmp_path):
    (tmp_path / "Dockerfile").write_text("FROM base\n")
    (tmp_path / "plugins").mkdir()

    from save_gcp_local.astro import scaffold_astro_project
    results = scaffold_astro_project(str(tmp_path), skip_compose=True)

    assert "docker-compose.override.yml" not in results
    assert not (tmp_path / "docker-compose.override.yml").exists()


def test_scaffold_force_overwrites(tmp_path):
    (tmp_path / "Dockerfile").write_text("FROM base\n")
    (tmp_path / "plugins").mkdir()
    plugin_path = tmp_path / "plugins" / "save_gcp_local_plugin.py"
    plugin_path.write_text("# old content\n")

    from save_gcp_local.astro import scaffold_astro_project
    results = scaffold_astro_project(str(tmp_path), force=True)

    assert results["plugins/save_gcp_local_plugin.py"] == "created"
    assert "airflow_plugin" in plugin_path.read_text()


# ----------------------------------------------------- connection patching
def test_apply_connection_overrides_sets_env():
    from save_gcp_local.connections import apply_connection_overrides

    count = apply_connection_overrides(
        overrides={"test_conn_xyz": {"conn_type": "postgres", "host": "localhost"}},
        include_defaults=False,
    )

    assert count == 1
    assert "AIRFLOW_CONN_TEST_CONN_XYZ" in os.environ
    val = os.environ["AIRFLOW_CONN_TEST_CONN_XYZ"]
    assert val.startswith("postgres://")
    assert "localhost" in val

    os.environ.pop("AIRFLOW_CONN_TEST_CONN_XYZ", None)


def test_apply_defaults_creates_gcp_connection():
    from save_gcp_local.connections import apply_connection_overrides

    # Clear any existing
    os.environ.pop("AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT", None)

    count = apply_connection_overrides(include_defaults=True)
    assert count >= 1
    assert "AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT" in os.environ

    os.environ.pop("AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT", None)
    os.environ.pop("AIRFLOW_CONN_GOOGLE_CLOUD_DATAPROC_DEFAULT", None)


def test_apply_skips_existing_env():
    from save_gcp_local.connections import apply_connection_overrides

    os.environ["AIRFLOW_CONN_SKIPME"] = "already://set"
    os.environ.pop("DPL_FORCE_CONNECTIONS", None)

    count = apply_connection_overrides(
        overrides={"skipme": {"conn_type": "postgres"}},
        include_defaults=False,
    )
    assert count == 0
    assert os.environ["AIRFLOW_CONN_SKIPME"] == "already://set"

    os.environ.pop("AIRFLOW_CONN_SKIPME", None)


def test_apply_force_overrides_existing():
    from save_gcp_local.connections import apply_connection_overrides

    os.environ["AIRFLOW_CONN_FORCEME"] = "old://value"
    os.environ["DPL_FORCE_CONNECTIONS"] = "true"

    count = apply_connection_overrides(
        overrides={"forceme": {"conn_type": "postgres", "host": "new-host"}},
        include_defaults=False,
    )
    assert count == 1
    assert "new-host" in os.environ["AIRFLOW_CONN_FORCEME"]

    os.environ.pop("AIRFLOW_CONN_FORCEME", None)
    os.environ.pop("DPL_FORCE_CONNECTIONS", None)


def test_dpl_conn_env_vars():
    from save_gcp_local.connections import apply_connection_overrides

    os.environ["DPL_CONN_MY_DB"] = json.dumps({
        "conn_type": "mysql",
        "host": "localhost",
        "port": 3306,
    })
    os.environ.pop("AIRFLOW_CONN_MY_DB", None)

    count = apply_connection_overrides(overrides={}, include_defaults=False)
    assert count >= 1
    assert "AIRFLOW_CONN_MY_DB" in os.environ

    os.environ.pop("AIRFLOW_CONN_MY_DB", None)
    os.environ.pop("DPL_CONN_MY_DB", None)


def test_load_connections_file(tmp_path):
    from save_gcp_local.connections import load_connections_file

    conn_file = tmp_path / "connections.json"
    data = {
        "my_db": {"conn_type": "postgres", "host": "localhost"},
        "other_db": {"conn_type": "mysql", "host": "db-host"},
    }
    conn_file.write_text(json.dumps(data))

    result = load_connections_file(str(conn_file))
    assert len(result) == 2
    assert result["my_db"]["conn_type"] == "postgres"


def test_build_connection_uri():
    from save_gcp_local.connections import build_connection_uri

    uri = build_connection_uri({
        "conn_type": "postgres",
        "host": "myhost",
        "port": 5432,
        "login": "user",
        "password": "pass",
        "schema": "mydb",
    })
    assert uri.startswith("postgres://")
    assert "user:pass@myhost:5432/mydb" in uri


def test_build_connection_uri_minimal():
    from save_gcp_local.connections import build_connection_uri

    uri = build_connection_uri({"conn_type": "google_cloud_platform"})
    assert uri == "google_cloud_platform://"


# --------------------------------------------------------- CLI integration
def test_cli_init_astro_no_dockerfile():
    from save_gcp_local.cli import main
    rc = main(["init-astro", "/nonexistent/path"])
    assert rc == 2


def test_cli_init_astro_full(tmp_path):
    (tmp_path / "Dockerfile").write_text("FROM quay.io/astronomer/astro-runtime:12.0.0\n")
    (tmp_path / "plugins").mkdir()
    (tmp_path / "requirements.txt").write_text("")

    from save_gcp_local.cli import main
    rc = main(["init-astro", str(tmp_path)])
    assert rc == 0

    assert (tmp_path / "plugins" / "save_gcp_local_plugin.py").exists()
    assert "DPL_ENABLED" in (tmp_path / ".env").read_text()
