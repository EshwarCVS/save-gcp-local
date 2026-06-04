"""Airflow plugin entrypoint.

Two ways this gets loaded:

1. Drop-in file: copy this module's import into $AIRFLOW_HOME/plugins/ (or set
   plugins path), and Airflow imports it at startup.

2. Entry point: Airflow also discovers plugins via the
   'airflow.plugins' entry point group (see pyproject if enabled).

Either way, importing it calls apply_patches().
"""

from __future__ import annotations

import logging

log = logging.getLogger("save_gcp_local.plugin")

# Apply patches on import.
try:
    from .airflow_patch import apply_patches
    apply_patches()
except Exception as e:  # never break Airflow startup
    log.warning("[save-gcp-local] patch on import failed: %s", e)


# Register a named Airflow plugin so Airflow logs its presence.
try:
    from airflow.plugins_manager import AirflowPlugin

    class DataprocLocalPlugin(AirflowPlugin):
        name = "save_gcp_local"
except Exception:
    # Not inside Airflow; the patch (if applicable) already ran above.
    pass
