"""Tests for the generic, Airflow-independent core."""

import os

from save_gcp_local.config import Config
from save_gcp_local.runner import SparkRunner


def make_runner(tmp_path, runner="local", dry_run=True):
    os.environ["DPL_JOBS_DIR"] = str(tmp_path / "jobs")
    os.environ["DPL_DATA_DIR"] = str(tmp_path / "data")
    os.environ["DPL_OUTPUT_DIR"] = str(tmp_path / "out")
    os.environ["DPL_RUNNER"] = runner
    os.environ["DPL_DRY_RUN"] = "true" if dry_run else "false"
    return SparkRunner(Config())


def test_rewrite_gs_path(tmp_path):
    r = make_runner(tmp_path)
    assert r.rewrite_path("gs://bucket/events/d.csv") == "/data/events/d.csv"
    assert r.rewrite_path("s3://b/x/y.parquet") == "/data/x/y.parquet"
    assert r.rewrite_path("/already/local") == "/already/local"
    assert r.rewrite_path("--flag") == "--flag"


def test_build_pyspark_cmd_local(tmp_path):
    r = make_runner(tmp_path, runner="local")
    cmd = r.build_pyspark("gs://b/main.py", ["--in", "gs://b/data/x.csv"], ["gs://b/lib.py"])
    assert "spark-submit" in cmd
    assert "--py-files" in cmd
    assert "/jobs/main.py" in cmd
    # arg path rewritten
    assert "/data/data/x.csv" in cmd


def test_build_spark_jar_cmd_docker(tmp_path):
    r = make_runner(tmp_path, runner="docker")
    cmd = r.build_spark_jar("com.x.Job", ["gs://b/job.jar"], ["--date", "2024-01-01"])
    assert cmd[0] in ("docker", "podman")
    assert "--class" in cmd
    assert "/jobs/job.jar" in cmd


def test_run_job_spec_pyspark_dry(tmp_path):
    r = make_runner(tmp_path, dry_run=True)
    job = {"pyspark_job": {"main_python_file_uri": "gs://b/m.py", "args": ["--x", "1"]}}
    assert r.run_job_spec(job, "t") == 0


def test_run_job_spec_unknown(tmp_path):
    r = make_runner(tmp_path)
    assert r.run_job_spec({"weird_job": {}}, "t") is None


def test_sample_provider(tmp_path):
    import pandas as pd
    src = tmp_path / "real.csv"
    pd.DataFrame({"a": range(1000)}).to_csv(src, index=False)
    from save_gcp_local.providers import get_provider
    dest = str(tmp_path / "sample.csv")
    get_provider("sample").materialize(str(src), dest, pct=10, seed=1)
    out = pd.read_csv(dest)
    assert 50 < len(out) < 200  # ~10% of 1000


def test_synthetic_provider_shape(tmp_path):
    import pandas as pd
    import numpy as np
    rng = np.random.default_rng(0)
    src = tmp_path / "real.csv"
    pd.DataFrame({
        "amt": rng.normal(100, 20, 2000),
        "cat": rng.choice(["A", "B"], 2000, p=[0.7, 0.3]),
    }).to_csv(src, index=False)
    from save_gcp_local.providers import get_provider
    dest = str(tmp_path / "synth.csv")
    get_provider("synthetic").materialize(str(src), dest, rows=4000, seed=0)
    out = pd.read_csv(dest)
    assert len(out) == 4000
    assert abs(out.amt.mean() - 100) < 5


# ---------------------------------------------------------------- resolver
def test_resolver_finds_job_in_repo_subfolder(tmp_path):
    # Simulate an Airflow repo: repo/dags + repo/jobs
    repo = tmp_path / "airflow_repo"
    (repo / "dags").mkdir(parents=True)
    (repo / "jobs").mkdir(parents=True)
    job = repo / "jobs" / "transform.py"
    job.write_text("print('hi')")

    from save_gcp_local.resolver import JobResolver
    # roots = the repo root; resolver tries jobs/ subdir automatically
    r = JobResolver([str(repo)])
    # operator referenced it as a relative path or a gs:// path — both resolve
    assert r.resolve("jobs/transform.py") == str(job)
    assert r.resolve("gs://bucket/whatever/transform.py") == str(job)  # by basename
    assert r.resolve("transform.py") == str(job)


def test_resolver_external_jar(tmp_path):
    other = tmp_path / "other_repo" / "target"
    other.mkdir(parents=True)
    jar = other / "job.jar"
    jar.write_text("fake")

    from save_gcp_local.resolver import JobResolver
    r = JobResolver([str(tmp_path / "airflow_repo"), str(other)])
    assert r.resolve("gs://b/job.jar") == str(jar)


def test_runner_mounts_external_dir(tmp_path):
    # Job lives outside jobs_dir -> runner must add an extra -v mount
    repo = tmp_path / "repo"
    (repo / "jobs").mkdir(parents=True)
    job = repo / "jobs" / "t.py"
    job.write_text("x")

    os.environ["DPL_JOBS_DIR"] = str(tmp_path / "empty_jobs")
    os.environ["DPL_DATA_DIR"] = str(tmp_path / "data")
    os.environ["DPL_OUTPUT_DIR"] = str(tmp_path / "out")
    os.environ["DPL_RUNNER"] = "docker"
    os.environ["DPL_DRY_RUN"] = "true"

    from save_gcp_local.resolver import JobResolver
    runner = SparkRunner(Config(), resolver=JobResolver([str(repo)]))
    cmd = runner.build_pyspark("jobs/t.py", ["--in", "gs://b/x.csv"])
    joined = " ".join(cmd)
    # external dir mounted, main file referenced under its token
    assert ":/jobs/ext0:ro" in joined
    assert "/jobs/ext0/t.py" in joined


def test_container_engine_explicit(tmp_path):
    import os as _os
    _os.environ["DPL_CONTAINER_ENGINE"] = "podman"
    _os.environ["DPL_RUNNER"] = "docker"
    r = make_runner(tmp_path, runner="docker")
    # explicit podman overrides auto-detect
    assert r.cfg.resolve_engine() == "podman"
    cmd = r.build_spark_jar("c.X", ["gs://b/j.jar"], [])
    assert cmd[0] == "podman"
    _os.environ.pop("DPL_CONTAINER_ENGINE", None)


def test_spark_submit_cmd_override(tmp_path):
    os.environ["DPL_SPARK_SUBMIT_CMD"] = "/opt/spark/bin/spark-submit"
    os.environ["DPL_RUNNER"] = "local"
    os.environ["DPL_DRY_RUN"] = "true"
    r = make_runner(tmp_path, runner="local")
    cmd = r.build_pyspark("gs://b/main.py", [])
    assert cmd[0] == "/opt/spark/bin/spark-submit"
    os.environ.pop("DPL_SPARK_SUBMIT_CMD", None)


def test_docker_entrypoint_in_cmd(tmp_path):
    os.environ["DPL_CONTAINER_ENGINE"] = "docker"
    os.environ["DPL_DOCKER_ENTRYPOINT"] = "/bin/sh"
    r = make_runner(tmp_path, runner="docker")
    cmd = r.build_spark_jar("com.X", ["gs://b/j.jar"], [])
    joined = " ".join(cmd)
    assert "--entrypoint /bin/sh" in joined
    os.environ.pop("DPL_DOCKER_ENTRYPOINT", None)
    os.environ.pop("DPL_CONTAINER_ENGINE", None)


def test_docker_entrypoint_absent_by_default(tmp_path):
    os.environ["DPL_CONTAINER_ENGINE"] = "docker"
    os.environ.pop("DPL_DOCKER_ENTRYPOINT", None)
    r = make_runner(tmp_path, runner="docker")
    cmd = r.build_spark_jar("com.X", ["gs://b/j.jar"], [])
    assert "--entrypoint" not in cmd
    os.environ.pop("DPL_CONTAINER_ENGINE", None)


def test_mock_stubs_installed_when_provider_missing(tmp_path):
    """apply_patches() should install sys.modules stubs when google provider absent."""
    import sys
    import types

    mod_path = "airflow.providers.google.cloud.operators.dataproc"

    # Remove any existing entry so we can simulate a missing provider
    saved = {k: v for k, v in sys.modules.items() if k.startswith("airflow")}
    for k in list(sys.modules.keys()):
        if k.startswith("airflow"):
            del sys.modules[k]

    # Also reset the _PATCHED flag so apply_patches runs fresh
    import save_gcp_local.airflow_patch as ap
    ap._PATCHED = False

    os.environ["DPL_ENABLED"] = "true"
    from save_gcp_local.config import Config
    from save_gcp_local.airflow_patch import apply_patches

    apply_patches(Config())

    assert mod_path in sys.modules, "Stub module should be registered in sys.modules"
    mod = sys.modules[mod_path]
    assert hasattr(mod, "DataprocSubmitJobOperator"), "Stub class should exist"
    assert hasattr(mod, "DataprocSubmitHiveJobOperator"), "Hive stub should exist"

    # Restore
    ap._PATCHED = False
    for k in list(sys.modules.keys()):
        if k.startswith("airflow"):
            del sys.modules[k]
    sys.modules.update(saved)


def test_extra_noop_operators_config(tmp_path):
    os.environ["DPL_EXTRA_NOOP_OPERATORS"] = "os.path.join"  # not a real operator, but importable
    from save_gcp_local.config import Config
    cfg = Config()
    assert "os.path.join" in cfg.extra_noop_operators
    os.environ.pop("DPL_EXTRA_NOOP_OPERATORS", None)


def test_extra_submit_operators_config(tmp_path):
    os.environ["DPL_EXTRA_SUBMIT_OPERATORS"] = "my.pkg.Op,other.pkg.Op2"
    from save_gcp_local.config import Config
    cfg = Config()
    assert cfg.extra_submit_operators == ["my.pkg.Op", "other.pkg.Op2"]
    os.environ.pop("DPL_EXTRA_SUBMIT_OPERATORS", None)


def test_connector_jars_config(tmp_path):
    os.environ["DPL_CONNECTOR_JARS"] = "/opt/jars/hive.jar,/opt/jars/os.jar"
    from save_gcp_local.config import Config
    cfg = Config()
    assert cfg.connector_jars == ["/opt/jars/hive.jar", "/opt/jars/os.jar"]
    os.environ.pop("DPL_CONNECTOR_JARS", None)


def test_spark_conf_config(tmp_path):
    os.environ["DPL_SPARK_CONF"] = "spark.hadoop.hive.metastore.uris=thrift://h:9083,spark.sql.catalogImplementation=hive"
    from save_gcp_local.config import Config
    cfg = Config()
    assert len(cfg.spark_conf) == 2
    assert "spark.hadoop.hive.metastore.uris=thrift://h:9083" in cfg.spark_conf
    os.environ.pop("DPL_SPARK_CONF", None)


def test_spark_conf_in_submit_cmd(tmp_path):
    os.environ["DPL_SPARK_CONF"] = "spark.sql.catalogImplementation=hive"
    r = make_runner(tmp_path, runner="local")
    cmd = r.build_pyspark("gs://b/main.py", [])
    assert "--conf" in cmd
    idx = cmd.index("--conf")
    assert cmd[idx + 1] == "spark.sql.catalogImplementation=hive"
    os.environ.pop("DPL_SPARK_CONF", None)


def test_connector_jars_in_submit_cmd(tmp_path):
    os.environ["DPL_CONNECTOR_JARS"] = "/opt/jars/test.jar"
    r = make_runner(tmp_path, runner="local")
    cmd = r.build_pyspark("gs://b/main.py", [])
    assert "--jars" in cmd
    idx = cmd.index("--jars")
    assert "/opt/jars/test.jar" in cmd[idx + 1]
    os.environ.pop("DPL_CONNECTOR_JARS", None)
