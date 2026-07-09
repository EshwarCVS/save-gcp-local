"""Airflow plugin entrypoint.

Two ways this gets loaded in Airflow 2.x:

1. Drop-in file: copy this module's import into $AIRFLOW_HOME/plugins/ and
   Airflow imports it at startup — before DAG parsing.

2. Entry point: Airflow also discovers plugins via the 'airflow.plugins'
   entry point group (see pyproject if enabled).

Airflow 3.x note
----------------
In Airflow 3.x the plugin loading order reversed: DAGs are parsed BEFORE
plugins load, so patches applied here arrive too late to intercept imports.
Run ``save-gcp-local install-airflow3`` once to install a .pth file that
triggers early patching at Python startup (before DAG parsing).
See SETUP.md section "Airflow 3.x" for full instructions.
"""

from __future__ import annotations

import logging

log = logging.getLogger("save_gcp_local.plugin")


def _detect_airflow_major() -> int:
    try:
        from airflow import __version__
        return int(__version__.split(".")[0])
    except Exception:
        return 2  # assume 2.x if we can't tell


# Apply patches on import.
try:
    from .airflow_patch import apply_patches
    apply_patches()
except Exception as e:  # never break Airflow startup
    log.warning("[save-gcp-local] patch on import failed: %s", e)

# Set up local connection overrides (GCP mock, local databases, etc.)
# so DAGs can resolve connections without real cloud credentials.
try:
    from .connections import setup_local_connections
    setup_local_connections()
except Exception as e:
    log.debug("[save-gcp-local] connection setup skipped: %s", e)

# Warn when running under Airflow 3.x where the plugin is too late.
if _detect_airflow_major() >= 3:
    log.warning(
        "[save-gcp-local] Airflow 3.x detected. Plugins load AFTER DAG parsing, "
        "so this plugin cannot patch operators before your DAG imports them. "
        "Run `save-gcp-local install-airflow3` once and set DPL_PATCH_EARLY=true "
        "in your Airflow environment to enable early patching. "
        "See SETUP.md § 'Airflow 3.x' for full instructions."
    )


# Register a named Airflow plugin so Airflow logs its presence.
try:
    from airflow.plugins_manager import AirflowPlugin

    class DataprocLocalPlugin(AirflowPlugin):
        name = "save_gcp_local"
except Exception:
    # Not inside Airflow; the patch (if applicable) already ran above.
    pass
