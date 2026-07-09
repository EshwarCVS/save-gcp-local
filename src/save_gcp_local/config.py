"""Configuration for save-gcp-local.

All settings come from environment variables so the same config works whether
the library is invoked via the CLI or auto-loaded as an Airflow plugin.
Nothing here is specific to any particular project or data layout.
"""

from __future__ import annotations

import logging
import os
import subprocess
from dataclasses import dataclass, field
from typing import List

log = logging.getLogger("save_gcp_local.config")


def _env_bool(name: str, default: bool) -> bool:
    val = os.environ.get(name)
    if val is None:
        return default
    return val.strip().lower() in ("1", "true", "yes", "on")


def _env_list(name: str, default: str = "") -> List[str]:
    raw = os.environ.get(name, default)
    return [x.strip() for x in raw.split(",") if x.strip()]


@dataclass
class Config:
    """Resolved runtime configuration.

    Environment variables (all optional, sensible defaults):
      DPL_ENABLED                turn interception on/off           (default: true)
      DPL_DOCKER_IMAGE           spark image                        (apache/spark:3.5.0)
      DPL_DOCKER_ENTRYPOINT      override container entrypoint      (default: not set)
      DPL_SPARK_SUBMIT_CMD       spark-submit path inside container (default: spark-submit)
      DPL_SPARK_MASTER           spark master                       (local[*])
      DPL_JOBS_DIR               host dir with job files -> /jobs
      DPL_DATA_DIR               host dir with input data -> /data
      DPL_OUTPUT_DIR             host dir for output    -> /output
      DPL_EXTRA_PACKAGES         comma list of --packages
      DPL_EXTRA_JARS             comma list of extra --jars (host paths, mounted)
      DPL_PATH_PREFIXES          comma list of remote prefixes to rewrite to /data
                                 (default: gs://,s3://,s3a://,abfs://,hdfs://)
      DPL_DOCKER_NETWORK         docker network (default: bridge)
      DPL_DOCKER_MEMORY          e.g. 8g (optional)
      DPL_DRY_RUN                print commands, do not execute     (default: false)
      DPL_RUNNER                 spark runner: docker | local        (default: docker)
      DPL_CONTAINER_ENGINE       auto | docker | podman              (default: auto)
      DPL_EXTRA_NOOP_OPERATORS   comma-sep FQCNs of custom operators to make no-ops
                                 e.g. bfdms.dpaas.BFDMSDataprocCreateClusterOperator
      DPL_EXTRA_SUBMIT_OPERATORS comma-sep FQCNs of custom operators to run locally
                                 e.g. my.pkg.CustomSubmitOperator
      DPL_CONNECTOR_JARS         comma list of connector JARs outside SPARK_HOME/jars/
      DPL_SPARK_CONF             comma list of key=value Spark --conf flags
                                 e.g. spark.hadoop.hive.metastore.uris=thrift://host:9083
    """

    enabled: bool = field(default_factory=lambda: _env_bool("DPL_ENABLED", True))
    docker_image: str = field(
        default_factory=lambda: os.environ.get("DPL_DOCKER_IMAGE", "apache/spark:3.5.0")
    )
    # Override the container entrypoint. Useful when the image's default entrypoint
    # doesn't expose spark-submit on PATH (e.g. the official apache/spark image).
    # Set to "" to clear the entrypoint, or a path like "/bin/sh".
    docker_entrypoint: str = field(
        default_factory=lambda: os.environ.get("DPL_DOCKER_ENTRYPOINT", "")
    )
    # spark-submit binary path inside the container. Override when the image puts
    # spark-submit somewhere other than the default PATH (e.g. /opt/spark/bin/spark-submit).
    spark_submit_cmd: str = field(
        default_factory=lambda: os.environ.get("DPL_SPARK_SUBMIT_CMD", "spark-submit")
    )
    spark_master: str = field(
        default_factory=lambda: os.environ.get("DPL_SPARK_MASTER", "local[*]")
    )
    jobs_dir: str = field(
        default_factory=lambda: os.path.abspath(os.environ.get("DPL_JOBS_DIR", "./jobs"))
    )
    data_dir: str = field(
        default_factory=lambda: os.path.abspath(os.environ.get("DPL_DATA_DIR", "./data"))
    )
    output_dir: str = field(
        default_factory=lambda: os.path.abspath(os.environ.get("DPL_OUTPUT_DIR", "./output"))
    )
    extra_packages: List[str] = field(default_factory=lambda: _env_list("DPL_EXTRA_PACKAGES"))
    extra_jars: List[str] = field(default_factory=lambda: _env_list("DPL_EXTRA_JARS"))
    path_prefixes: List[str] = field(
        default_factory=lambda: _env_list(
            "DPL_PATH_PREFIXES", "gs://,s3://,s3a://,abfs://,hdfs://"
        )
    )
    docker_network: str = field(
        default_factory=lambda: os.environ.get("DPL_DOCKER_NETWORK", "bridge")
    )
    docker_memory: str = field(default_factory=lambda: os.environ.get("DPL_DOCKER_MEMORY", ""))
    dry_run: bool = field(default_factory=lambda: _env_bool("DPL_DRY_RUN", False))
    runner: str = field(default_factory=lambda: os.environ.get("DPL_RUNNER", "docker"))
    # Which container CLI to invoke for the 'docker' runner. Podman is CLI-compatible
    # with docker, so 'podman' works as a drop-in. Default auto-detects.
    container_engine: str = field(
        default_factory=lambda: os.environ.get("DPL_CONTAINER_ENGINE", "auto")
    )
    # Connector JARs for Spark to talk to external services (Hive, OpenSearch,
    # databases).  JARs in $SPARK_HOME/jars/ are auto-classpathd; this list is
    # for JARs outside that directory.
    connector_jars: List[str] = field(
        default_factory=lambda: _env_list("DPL_CONNECTOR_JARS")
    )
    # Arbitrary Spark --conf key=value pairs.  Useful for Hive metastore URI,
    # catalog implementation, etc.
    spark_conf: List[str] = field(
        default_factory=lambda: _env_list("DPL_SPARK_CONF")
    )
    # Extra operators from custom packages to patch as no-ops or local submits.
    extra_noop_operators: List[str] = field(
        default_factory=lambda: _env_list("DPL_EXTRA_NOOP_OPERATORS")
    )
    extra_submit_operators: List[str] = field(
        default_factory=lambda: _env_list("DPL_EXTRA_SUBMIT_OPERATORS")
    )

    def resolve_engine(self) -> str:
        """Return the container CLI to use.

        For 'auto', checks binary existence AND connectivity so stale sockets
        from crashed VMs don't silently produce confusing errors.
        """
        if self.container_engine != "auto":
            return self.container_engine
        import shutil
        for cli in ("docker", "podman"):
            if not shutil.which(cli):
                continue
            try:
                result = subprocess.run(
                    [cli, "info"],
                    capture_output=True,
                    timeout=10,
                )
                if result.returncode == 0:
                    return cli
                log.warning(
                    "[save-gcp-local] %s binary found but daemon not responding (exit %d). "
                    "Run `%s machine start` or set DPL_CONTAINER_ENGINE explicitly.",
                    cli, result.returncode, cli,
                )
            except subprocess.TimeoutExpired:
                log.warning(
                    "[save-gcp-local] %s info timed out — daemon may be unreachable. "
                    "Set DPL_CONTAINER_ENGINE=docker or DPL_CONTAINER_ENGINE=podman.",
                    cli,
                )
            except OSError:
                pass
        return "docker"  # last resort; error surfaces clearly at container launch

    def ensure_dirs(self) -> None:
        for d in (self.jobs_dir, self.data_dir, self.output_dir):
            os.makedirs(d, exist_ok=True)


# A module-level singleton is convenient for the plugin path; the CLI builds
# its own and can override env first.
def load_config() -> Config:
    return Config()
