"""Airflow Variables auto-import for local development.

Airflow reads AIRFLOW_VAR_{NAME} environment variables as Variables, so DAGs
that call ``Variable.get("name")`` work without manual setup in the UI.

This module loads variables from a JSON file and/or DPL_VAR_* env vars and
sets the corresponding AIRFLOW_VAR_* entries.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Dict, Optional

log = logging.getLogger("save_gcp_local.variables")


def _var_env_key(var_name: str) -> str:
    return f"AIRFLOW_VAR_{var_name.upper()}"


def apply_variable_overrides(
    variables: Optional[Dict[str, str]] = None,
) -> int:
    """Set AIRFLOW_VAR_* env vars for local Airflow Variables.

    Returns the number of variables set.
    """
    merged: Dict[str, str] = {}
    if variables:
        merged.update(variables)

    for key, val in os.environ.items():
        if key.startswith("DPL_VAR_") and val:
            var_name = key[len("DPL_VAR_"):].lower()
            merged[var_name] = val

    count = 0
    force = os.environ.get("DPL_FORCE_VARIABLES", "").lower() in ("1", "true", "yes")
    for var_name, value in merged.items():
        env_key = _var_env_key(var_name)
        if env_key in os.environ and not force:
            log.debug(
                "[save-gcp-local] %s already set, skipping (set DPL_FORCE_VARIABLES=true to override)",
                env_key,
            )
            continue
        os.environ[env_key] = value if isinstance(value, str) else json.dumps(value)
        log.info("[save-gcp-local] Variable override: %s", var_name)
        count += 1

    return count


def load_variables_file(path: str) -> Dict[str, str]:
    """Load variable overrides from a JSON file.

    Format: {"var_name": "value", "json_var": {"nested": "object"}}
    Non-string values are JSON-serialized.
    """
    with open(path) as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Variables file must be a JSON object, got {type(data).__name__}")
    return {k: v if isinstance(v, str) else json.dumps(v) for k, v in data.items()}


def setup_local_variables(variables_file: Optional[str] = None) -> int:
    """One-call setup: load file if given, apply DPL_VAR_* overrides.

    Called by the Airflow plugin on startup.
    """
    filepath = variables_file or os.environ.get("DPL_VARIABLES_FILE")
    variables = None
    if filepath and os.path.isfile(filepath):
        try:
            variables = load_variables_file(filepath)
            log.info("[save-gcp-local] Loaded %d variable overrides from %s", len(variables), filepath)
        except Exception as e:
            log.warning("[save-gcp-local] Failed to load variables file %s: %s", filepath, e)

    return apply_variable_overrides(variables)
