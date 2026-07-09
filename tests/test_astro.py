"""Tests for Astro CLI scaffolding, connection patching, and variable support."""

import json
import os

import pytest


# --------------------------------------------------------- astro scaffolding
def test_scaffold_creates_all_files(tmp_path):
    """init-astro generates the expected files in the project directory."""
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
    assert results["include/local_variables.json"] == "created"
    assert results["requirements.txt"] == "updated"

    # Verify Dockerfile was appended
    content = (tmp_path / "Dockerfile").read_text()
    assert "save-gcp-local" in content
    assert "SPARK_HOME" in content
    assert "openjdk" in content
    assert "postgresql-42.7.1.jar" in content
    assert "mssql-jdbc" in content

    # Verify plugin file
    assert (tmp_path / "plugins" / "save_gcp_local_plugin.py").exists()
    plugin_content = (tmp_path / "plugins" / "save_gcp_local_plugin.py").read_text()
    assert "airflow_plugin" in plugin_content
    assert "setup_local_connections" in plugin_content
    assert "setup_local_variables" in plugin_content

    # Verify .env
    env_content = (tmp_path / ".env").read_text()
    assert "DPL_ENABLED=true" in env_content
    assert "DPL_RUNNER=local" in env_content
    assert "DPL_SPARK_SUBMIT_CMD" in env_content
    assert "DPL_SPARK_CONF" in env_content
    assert "hive.metastore.uris" in env_content
    assert "DPL_VARIABLES_FILE" in env_content

    # Verify docker-compose
    compose = (tmp_path / "docker-compose.override.yml").read_text()
    assert "local-postgres" in compose
    assert "local-hive" in compose
    assert "local-opensearch" in compose
    assert "local-mssql" in compose

    # Verify connections
    conn = json.loads((tmp_path / "include" / "local_connections.json").read_text())
    assert "google_cloud_default" in conn
    assert "local_postgres" in conn
    assert "hive_default" in conn
    assert "opensearch_default" in conn
    assert "mssql_default" in conn

    # Verify variables
    variables = json.loads((tmp_path / "include" / "local_variables.json").read_text())
    assert "hive_metastore_uri" in variables
    assert "opensearch_host" in variables
    assert "mssql_host" in variables
    assert "environment" in variables
    assert variables["environment"] == "local"

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
    assert results["include/local_variables.json"] == "exists"
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


def test_scaffold_engine_podman(tmp_path):
    """--engine podman sets DPL_CONTAINER_ENGINE in .env."""
    (tmp_path / "Dockerfile").write_text("FROM base\n")
    (tmp_path / "plugins").mkdir()

    from save_gcp_local.astro import scaffold_astro_project
    scaffold_astro_project(str(tmp_path), engine="podman")

    env_content = (tmp_path / ".env").read_text()
    assert "DPL_CONTAINER_ENGINE=podman" in env_content


def test_scaffold_engine_auto_commented(tmp_path):
    """Default engine=auto leaves DPL_CONTAINER_ENGINE commented out."""
    (tmp_path / "Dockerfile").write_text("FROM base\n")
    (tmp_path / "plugins").mkdir()

    from save_gcp_local.astro import scaffold_astro_project
    scaffold_astro_project(str(tmp_path), engine="auto")

    env_content = (tmp_path / ".env").read_text()
    assert "# DPL_CONTAINER_ENGINE=auto" in env_content


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

    for key in list(os.environ.keys()):
        if key.startswith("AIRFLOW_CONN_"):
            os.environ.pop(key, None)

    count = apply_connection_overrides(include_defaults=True)
    assert count >= 1
    assert "AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT" in os.environ

    for key in list(os.environ.keys()):
        if key.startswith("AIRFLOW_CONN_"):
            os.environ.pop(key, None)


def test_apply_defaults_includes_hive_opensearch_mssql():
    from save_gcp_local.connections import apply_connection_overrides

    for key in list(os.environ.keys()):
        if key.startswith("AIRFLOW_CONN_"):
            os.environ.pop(key, None)

    count = apply_connection_overrides(include_defaults=True)
    assert count >= 5
    assert "AIRFLOW_CONN_HIVE_DEFAULT" in os.environ
    assert "AIRFLOW_CONN_OPENSEARCH_DEFAULT" in os.environ
    assert "AIRFLOW_CONN_MSSQL_DEFAULT" in os.environ

    for key in list(os.environ.keys()):
        if key.startswith("AIRFLOW_CONN_"):
            os.environ.pop(key, None)


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


# --------------------------------------------------------- variables
def test_apply_variable_overrides():
    from save_gcp_local.variables import apply_variable_overrides

    os.environ.pop("AIRFLOW_VAR_TEST_VAR_XYZ", None)

    count = apply_variable_overrides({"test_var_xyz": "hello"})
    assert count == 1
    assert os.environ["AIRFLOW_VAR_TEST_VAR_XYZ"] == "hello"

    os.environ.pop("AIRFLOW_VAR_TEST_VAR_XYZ", None)


def test_dpl_var_env_vars():
    from save_gcp_local.variables import apply_variable_overrides

    os.environ["DPL_VAR_MY_SETTING"] = "value123"
    os.environ.pop("AIRFLOW_VAR_MY_SETTING", None)

    count = apply_variable_overrides()
    assert count >= 1
    assert os.environ["AIRFLOW_VAR_MY_SETTING"] == "value123"

    os.environ.pop("AIRFLOW_VAR_MY_SETTING", None)
    os.environ.pop("DPL_VAR_MY_SETTING", None)


def test_variable_skip_existing():
    from save_gcp_local.variables import apply_variable_overrides

    os.environ["AIRFLOW_VAR_SKIPVAR"] = "already_set"
    os.environ.pop("DPL_FORCE_VARIABLES", None)

    count = apply_variable_overrides({"skipvar": "new_value"})
    assert count == 0
    assert os.environ["AIRFLOW_VAR_SKIPVAR"] == "already_set"

    os.environ.pop("AIRFLOW_VAR_SKIPVAR", None)


def test_variable_force_override():
    from save_gcp_local.variables import apply_variable_overrides

    os.environ["AIRFLOW_VAR_FORCEVAR"] = "old_value"
    os.environ["DPL_FORCE_VARIABLES"] = "true"

    count = apply_variable_overrides({"forcevar": "new_value"})
    assert count == 1
    assert os.environ["AIRFLOW_VAR_FORCEVAR"] == "new_value"

    os.environ.pop("AIRFLOW_VAR_FORCEVAR", None)
    os.environ.pop("DPL_FORCE_VARIABLES", None)


def test_load_variables_file(tmp_path):
    from save_gcp_local.variables import load_variables_file

    var_file = tmp_path / "variables.json"
    data = {
        "string_var": "hello",
        "json_var": {"key": "value"},
        "number_var": 42,
    }
    var_file.write_text(json.dumps(data))

    result = load_variables_file(str(var_file))
    assert result["string_var"] == "hello"
    assert result["json_var"] == '{"key": "value"}'
    assert result["number_var"] == "42"


def test_setup_local_variables_with_file(tmp_path):
    from save_gcp_local.variables import setup_local_variables

    var_file = tmp_path / "variables.json"
    var_file.write_text(json.dumps({"test_setup_var": "from_file"}))

    os.environ.pop("AIRFLOW_VAR_TEST_SETUP_VAR", None)

    count = setup_local_variables(str(var_file))
    assert count >= 1
    assert os.environ["AIRFLOW_VAR_TEST_SETUP_VAR"] == "from_file"

    os.environ.pop("AIRFLOW_VAR_TEST_SETUP_VAR", None)


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
    assert (tmp_path / "include" / "local_variables.json").exists()


def test_cli_init_astro_with_engine(tmp_path):
    (tmp_path / "Dockerfile").write_text("FROM quay.io/astronomer/astro-runtime:12.0.0\n")
    (tmp_path / "plugins").mkdir()
    (tmp_path / "requirements.txt").write_text("")

    from save_gcp_local.cli import main
    rc = main(["init-astro", "--engine", "podman", str(tmp_path)])
    assert rc == 0

    env_content = (tmp_path / ".env").read_text()
    assert "DPL_CONTAINER_ENGINE=podman" in env_content
