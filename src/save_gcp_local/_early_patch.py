"""Early patching for Airflow 3.x compatibility.

Airflow 3.x parses DAGs BEFORE loading plugins, so the standard plugin
approach registers patches too late. This module is imported at Python
startup via a .pth file (installed by `save-gcp-local install-airflow3`)
so stubs and patches are in place before any DAG is parsed.

Only activates when DPL_PATCH_EARLY=true is set in the environment.
Set it inside Airflow's environment (airflow.cfg [core] / Kubernetes env /
Composer env var) rather than globally so it doesn't affect unrelated
Python processes on the same machine.
"""

from __future__ import annotations

import os

if os.environ.get("DPL_PATCH_EARLY", "").strip().lower() in ("1", "true", "yes", "on"):
    try:
        from save_gcp_local.airflow_patch import apply_patches
        apply_patches()
    except Exception:
        pass
