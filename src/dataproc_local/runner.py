"""Generic local Spark runner.

Parses a Dataproc-style job specification (the dict shape used by
DataprocSubmitJobOperator and the legacy operators) and runs it locally,
either inside a Docker container or via a local spark-submit.

This module knows nothing about Airflow — it only understands job specs and
how to run spark-submit. That keeps it reusable and testable on its own.
"""

from __future__ import annotations

import logging
import os
import shlex
import subprocess
from typing import Dict, List, Optional

from .config import Config
from .resolver import JobResolver, build_default_roots

log = logging.getLogger("dataproc_local.runner")


class SparkRunner:
    def __init__(self, config: Config, resolver: "JobResolver" = None):
        self.cfg = config
        # Resolver finds job files across many roots (Airflow repo subfolders,
        # extra dirs, JARs elsewhere). If none supplied, build the default set.
        self.resolver = resolver or JobResolver(build_default_roots(config))
        # Extra host dirs to mount into /jobs/<token> when a resolved file lives
        # outside the configured jobs_dir (e.g. a JAR in another repo).
        self._extra_mounts: Dict[str, str] = {}

    # ----------------------------------------------------------------- paths
    def rewrite_path(self, value):
        """Rewrite a remote URI (gs://, s3://, ...) to a local /data path.

        gs://bucket/a/b.csv -> /data/a/b.csv
        Anything not matching a known prefix is returned unchanged.
        """
        if not isinstance(value, str):
            return value
        for prefix in self.cfg.path_prefixes:
            if value.startswith(prefix):
                rest = value[len(prefix):]
                # drop the bucket/authority segment, keep the key path
                parts = rest.split("/", 1)
                key = parts[1] if len(parts) > 1 else parts[0]
                return f"/data/{key}"
        return value

    def _localize_main(self, uri: str) -> str:
        """Resolve a job file (anywhere) to its container path.

        - If the file is found on disk, its directory is mounted into the
          container and we return the in-container path. This lets jobs live in
          the Airflow repo, a subfolder, or a separate repo / JAR location.
        - If not found, we assume it's already present in the mounted jobs_dir
          and fall back to /jobs/<basename>.
        """
        found = self.resolver.resolve(uri)
        if found:
            d = os.path.dirname(found)
            base = os.path.basename(found)
            if os.path.abspath(d) == os.path.abspath(self.cfg.jobs_dir):
                return f"/jobs/{base}"
            # Mount this external directory under a stable token.
            token = self._mount_token(d)
            return f"/jobs/{token}/{base}"
        return "/jobs/" + os.path.basename(self.resolver._strip_remote(uri))

    def _mount_token(self, host_dir: str) -> str:
        """Register an extra host dir to mount; return its container subfolder."""
        host_dir = os.path.abspath(host_dir)
        for token, d in self._extra_mounts.items():
            if d == host_dir:
                return token
        token = f"ext{len(self._extra_mounts)}"
        self._extra_mounts[token] = host_dir
        return token

    # --------------------------------------------------------------- command
    def _spark_submit_prefix(self) -> List[str]:
        cmd = ["spark-submit", "--master", self.cfg.spark_master]
        if self.cfg.extra_packages:
            cmd += ["--packages", ",".join(self.cfg.extra_packages)]
        if self.cfg.extra_jars:
            jars = [self._localize_main(j) for j in self.cfg.extra_jars]
            cmd += ["--jars", ",".join(jars)]
        return cmd

    def _docker_prefix(self) -> List[str]:
        engine = self.cfg.resolve_engine()
        cmd = [engine, "run", "--rm", "--network", self.cfg.docker_network]
        if self.cfg.docker_memory:
            cmd += ["--memory", self.cfg.docker_memory]
        cmd += [
            "-v", f"{self.cfg.jobs_dir}:/jobs",
            "-v", f"{self.cfg.data_dir}:/data",
            "-v", f"{self.cfg.output_dir}:/output",
        ]
        # Mount any external job/JAR directories discovered by the resolver.
        for token, host_dir in self._extra_mounts.items():
            cmd += ["-v", f"{host_dir}:/jobs/{token}:ro"]
        cmd += ["-w", "/jobs", self.cfg.docker_image]
        return cmd

    def _wrap(self, spark_cmd: List[str]) -> List[str]:
        if self.cfg.runner == "local":
            return spark_cmd
        return self._docker_prefix() + spark_cmd

    # ------------------------------------------------------------ build cmds
    def build_pyspark(self, main_uri: str, args, py_files=None) -> List[str]:
        cmd = self._spark_submit_prefix()
        if py_files:
            joined = ",".join(self._localize_main(p) for p in py_files)
            cmd += ["--py-files", joined]
        cmd += [self._localize_main(main_uri)]
        cmd += [str(self.rewrite_path(a)) for a in (args or [])]
        return self._wrap(cmd)

    def build_spark_jar(self, main_class, jar_uris, args) -> List[str]:
        cmd = self._spark_submit_prefix()
        jars = [self._localize_main(j) for j in (jar_uris or []) if j]
        if main_class:
            cmd += ["--class", main_class]
        if len(jars) > 1:
            cmd += ["--jars", ",".join(jars[1:])]
        if jars:
            cmd += [jars[0]]
        cmd += [str(self.rewrite_path(a)) for a in (args or [])]
        return self._wrap(cmd)

    def build_spark_sql(self, query: str) -> List[str]:
        return self._wrap(["spark-sql", "-e", query])

    # --------------------------------------------------------------- execute
    def run_cmd(self, cmd: List[str], label: str) -> int:
        self.cfg.ensure_dirs()
        printable = " ".join(shlex.quote(c) for c in cmd)
        log.info("[dataproc-local] %s -> running locally (runner=%s)", label, self.cfg.runner)
        log.info("[dataproc-local] %s", printable)
        if self.cfg.dry_run:
            log.info("[dataproc-local] DRY RUN — not executing.")
            return 0
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            log.info("[spark] %s", line.rstrip())
        proc.wait()
        if proc.returncode != 0:
            raise RuntimeError(f"[dataproc-local] {label} failed (exit {proc.returncode})")
        log.info("[dataproc-local] %s completed.", label)
        return proc.returncode

    # ------------------------------------------------- high-level job parsing
    def run_job_spec(self, job: Dict, label: str = "job") -> Optional[int]:
        """Run a Dataproc-style job dict. Returns exit code or None if unknown."""
        if "pyspark_job" in job:
            j = job["pyspark_job"]
            cmd = self.build_pyspark(
                j.get("main_python_file_uri"),
                j.get("args"),
                j.get("python_file_uris"),
            )
            return self.run_cmd(cmd, f"{label} (pyspark)")
        if "spark_job" in job:
            j = job["spark_job"]
            cmd = self.build_spark_jar(
                j.get("main_class"),
                j.get("jar_file_uris"),
                j.get("args"),
            )
            return self.run_cmd(cmd, f"{label} (spark)")
        if "spark_sql_job" in job:
            j = job["spark_sql_job"]
            queries = j.get("query_list", {}).get("queries", [])
            query = queries[0] if queries else j.get("query_file_uri", "")
            cmd = self.build_spark_sql(query)
            return self.run_cmd(cmd, f"{label} (spark-sql)")
        log.warning(
            "[dataproc-local] Unknown job type for %s (keys=%s) — skipping.",
            label, list(job.keys()),
        )
        return None
