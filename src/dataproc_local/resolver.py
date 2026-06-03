"""Job-file resolution.

Job files (PySpark .py, Spark/Scala .jar, etc.) can live in many places:
  - a subfolder inside the Airflow repo (dags/, jobs/, spark/, include/, plugins/)
  - a JAR somewhere on disk
  - a separate repo
  - a remote URI (gs://, s3://) that we map to a local staged copy

This module resolves whatever string the operator gives us to a real local
file, searching a configurable list of roots. It assumes nothing about a single
fixed layout — it tries several and uses the first match.
"""

from __future__ import annotations

import logging
import os
from typing import List, Optional

log = logging.getLogger("dataproc_local.resolver")

# Common subfolders where jobs live inside an Airflow project.
DEFAULT_SUBDIRS = ["", "jobs", "spark", "include", "dags", "plugins", "src", "tasks"]

# Remote prefixes we strip down to a basename and look up locally.
_REMOTE_PREFIXES = ("gs://", "s3://", "s3a://", "abfs://", "abfss://", "hdfs://", "file://", "wasbs://")


class JobResolver:
    """Resolve operator-supplied job references to real local paths.

    roots:      ordered list of directories to search (most specific first)
    subdirs:    subfolders tried under each root
    """

    def __init__(self, roots: List[str], subdirs: Optional[List[str]] = None):
        # de-dup while preserving order; keep only existing dirs but remember all
        self.roots = [r for r in dict.fromkeys(os.path.abspath(x) for x in roots if x)]
        self.subdirs = subdirs or DEFAULT_SUBDIRS

    # ---------------------------------------------------------------- helpers
    @staticmethod
    def is_remote(ref: str) -> bool:
        return isinstance(ref, str) and ref.startswith(_REMOTE_PREFIXES)

    @staticmethod
    def _strip_remote(ref: str) -> str:
        for p in _REMOTE_PREFIXES:
            if ref.startswith(p):
                rest = ref[len(p):]
                # drop bucket/authority, keep the key path
                parts = rest.split("/", 1)
                return parts[1] if len(parts) > 1 else parts[0]
        return ref

    # ----------------------------------------------------------------- search
    def _candidates(self, ref: str) -> List[str]:
        """Generate candidate local paths for a job reference, in priority order."""
        cands: List[str] = []

        # 1. Absolute path that already exists.
        if os.path.isabs(ref) and os.path.exists(ref):
            cands.append(ref)

        # 2. Path as-is relative to CWD.
        if os.path.exists(ref):
            cands.append(os.path.abspath(ref))

        # The "logical" path we search for under each root: for remote URIs use
        # the key path; otherwise use the ref itself and also just its basename.
        logical = self._strip_remote(ref) if self.is_remote(ref) else ref
        base = os.path.basename(logical)

        for root in self.roots:
            for sub in self.subdirs:
                base_dir = os.path.join(root, sub) if sub else root
                # full logical path under this root (preserves any subpath)
                cands.append(os.path.join(base_dir, logical))
                # just the basename under this root (handles flattened layouts)
                cands.append(os.path.join(base_dir, base))

        return cands

    def resolve(self, ref: str) -> Optional[str]:
        """Return the first existing local path for `ref`, or None.

        On None, the caller can decide to fall back to mounting by basename
        (the container path), which is what the runner does.
        """
        if not ref:
            return None
        for cand in self._candidates(ref):
            if os.path.isfile(cand):
                log.debug("[dataproc-local] resolved %r -> %s", ref, cand)
                return cand
        log.debug("[dataproc-local] could not resolve %r locally", ref)
        return None

    def resolve_or_basename(self, ref: str) -> str:
        """Resolve to a real local file path, else fall back to basename.

        The runner mounts the directory of the resolved file into the container,
        or — if unresolved — assumes the file is already present in the mounted
        jobs dir by basename.
        """
        found = self.resolve(ref)
        if found:
            return found
        return os.path.basename(self._strip_remote(ref))


def build_default_roots(config) -> List[str]:
    """Assemble search roots from config + Airflow env, most specific first.

    Order:
      1. explicit DPL_JOBS_DIR (if set)
      2. DPL_JOBS_PATH extra roots (comma list)
      3. the Airflow DAGs folder and its parent (the Airflow repo root)
      4. AIRFLOW_HOME
      5. current working directory
    """
    roots: List[str] = []

    jobs_dir = getattr(config, "jobs_dir", None)
    if jobs_dir:
        roots.append(jobs_dir)

    extra = os.environ.get("DPL_JOBS_PATH", "")
    roots += [x.strip() for x in extra.split(",") if x.strip()]

    dags_folder = os.environ.get("AIRFLOW__CORE__DAGS_FOLDER")
    if dags_folder:
        roots.append(dags_folder)
        roots.append(os.path.dirname(dags_folder.rstrip("/")))  # repo root

    airflow_home = os.environ.get("AIRFLOW_HOME")
    if airflow_home:
        roots.append(airflow_home)

    roots.append(os.getcwd())
    return roots
