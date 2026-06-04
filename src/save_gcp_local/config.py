"""Configuration for save-gcp-local.

All settings come from environment variables so the same config works whether
the library is invoked via the CLI or auto-loaded as an Airflow plugin.
Nothing here is specific to any particular project or data layout.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import List


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
      DPL_ENABLED            turn interception on/off          (default: true)
      DPL_DOCKER_IMAGE       spark image                       (apache/spark:3.5.0-python3)
      DPL_SPARK_MASTER       spark master                      (local[*])
      DPL_JOBS_DIR           host dir with job files -> /jobs
      DPL_DATA_DIR           host dir with input data -> /data
      DPL_OUTPUT_DIR         host dir for output    -> /output
      DPL_EXTRA_PACKAGES     comma list of --packages
      DPL_EXTRA_JARS         comma list of extra --jars (host paths, mounted)
      DPL_PATH_PREFIXES      comma list of remote prefixes to rewrite to /data
                             (default: gs://,s3://,s3a://,abfs://,hdfs://)
      DPL_DOCKER_NETWORK     docker network (default: bridge)
      DPL_DOCKER_MEMORY      e.g. 8g (optional)
      DPL_DRY_RUN            print commands, do not execute     (default: false)
      DPL_RUNNER            spark runner: docker | local        (default: docker)
    """

    enabled: bool = field(default_factory=lambda: _env_bool("DPL_ENABLED", True))
    docker_image: str = field(
        default_factory=lambda: os.environ.get("DPL_DOCKER_IMAGE", "apache/spark:3.5.0-python3")
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

    def resolve_engine(self) -> str:
        """Return the container CLI to use. 'auto' picks docker, else podman."""
        if self.container_engine != "auto":
            return self.container_engine
        import shutil
        for cli in ("docker", "podman"):
            if shutil.which(cli):
                return cli
        return "docker"  # fall back; error surfaces clearly at run time

    def ensure_dirs(self) -> None:
        for d in (self.jobs_dir, self.data_dir, self.output_dir):
            os.makedirs(d, exist_ok=True)


# A module-level singleton is convenient for the plugin path; the CLI builds
# its own and can override env first.
def load_config() -> Config:
    return Config()
