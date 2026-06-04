"""Airflow integration: monkey-patch Dataproc operators to run locally.

Importing this module (which the Airflow plugin and the CLI both do) replaces
`.execute()` on the Dataproc operators so cluster lifecycle becomes a no-op and
job submission is routed to the local SparkRunner.

Generic: it patches whatever Dataproc operators exist in the installed provider
version, and reads job specs from the attributes used across operator versions.

When apache-airflow-providers-google is not installed, this module installs
minimal stub classes in sys.modules so DAGs can still import and parse operator
classes without raising ModuleNotFoundError.  The stubs' execute() methods are
immediately replaced by the patches below, so the DAG runs locally end-to-end.
"""

from __future__ import annotations

import importlib
import logging
import sys
import types

from .config import Config, load_config
from .runner import SparkRunner

log = logging.getLogger("save_gcp_local.airflow_patch")

_PATCHED = False

# All operator class names the library knows about (used for stub generation too).
_NOOP_OPERATOR_NAMES = [
    "DataprocCreateClusterOperator",
    "DataprocDeleteClusterOperator",
    "DataprocUpdateClusterOperator",
    "DataprocStartClusterOperator",
    "DataprocStopClusterOperator",
    "DataprocInstantiateWorkflowTemplateOperator",
    "DataprocInstantiateInlineWorkflowTemplateOperator",
    # Hive jobs create/query tables; locally Spark uses an embedded Derby
    # metastore so DDL cannot run meaningfully — treat as no-op and log the HQL.
    "DataprocSubmitHiveJobOperator",
]

_SUBMIT_OPERATOR_NAMES = [
    "DataprocSubmitJobOperator",
    "DataprocSubmitPySparkJobOperator",
    "DataprocSubmitSparkJobOperator",
    "DataprocSubmitSparkSqlJobOperator",
    "DataprocSubmitHadoopJobOperator",
    "DataprocCreateBatchOperator",
]

_ALL_OPERATOR_NAMES = _NOOP_OPERATOR_NAMES + _SUBMIT_OPERATOR_NAMES


def _noop_execute(label: str):
    def execute(self, context):  # noqa: ANN001
        log.info(
            "[save-gcp-local] %s on task '%s' -> SKIPPED (no GCP cluster, no cost).",
            label, getattr(self, "task_id", "?"),
        )
        return {"save_gcp_local": "skipped", "operator": label}
    return execute


def _hive_noop_execute(label: str):
    """No-op for Hive operators that logs the HQL so users can verify what was skipped."""
    def execute(self, context):  # noqa: ANN001
        query = (
            getattr(self, "query", None)
            or getattr(self, "hql", None)
            or getattr(self, "query_file_uri", "")
        )
        preview = (str(query)[:200] + "…") if len(str(query)) > 200 else str(query)
        log.info(
            "[save-gcp-local] %s on task '%s' -> SKIPPED locally "
            "(Hive metastore unavailable in local Spark). HQL: %s",
            label, getattr(self, "task_id", "?"), preview or "(none)",
        )
        return {"save_gcp_local": "skipped", "operator": label}
    return execute


def _submit_execute(runner: SparkRunner):
    def execute(self, context):  # noqa: ANN001
        task_id = getattr(self, "task_id", "submit_job")
        # Modern operator: a `job` dict
        job = getattr(self, "job", None) or getattr(self, "_job", None)
        if isinstance(job, dict):
            return runner.run_job_spec(job, task_id)
        # Legacy PySpark operator
        if hasattr(self, "main") and getattr(self, "main"):
            cmd = runner.build_pyspark(
                getattr(self, "main"),
                getattr(self, "arguments", None),
                getattr(self, "pyfiles", None),
            )
            return runner.run_cmd(cmd, f"{task_id} (pyspark-legacy)")
        # Legacy Spark/Scala operator
        if getattr(self, "main_class", None) or getattr(self, "main_jar", None):
            jars = getattr(self, "dataproc_jars", None) or [getattr(self, "main_jar", "")]
            cmd = runner.build_spark_jar(
                getattr(self, "main_class", None),
                jars,
                getattr(self, "arguments", None),
            )
            return runner.run_cmd(cmd, f"{task_id} (spark-legacy)")
        log.warning("[save-gcp-local] No job spec found on task '%s'; skipping.", task_id)
        return None
    return execute


def _install_mock_stubs() -> types.ModuleType:
    """Install minimal stub classes in sys.modules for the Dataproc operators module.

    Called when apache-airflow-providers-google is not installed.  Stubs let DAG
    files do ``from airflow.providers.google.cloud.operators.dataproc import
    DataprocSubmitJobOperator`` without raising ModuleNotFoundError.  The stub
    execute() methods are replaced immediately by apply_patches() below.
    """
    mod_path = "airflow.providers.google.cloud.operators.dataproc"

    # Ensure every ancestor module exists in sys.modules so Python's import
    # machinery can resolve the full dotted path.
    parts = mod_path.split(".")
    for i in range(1, len(parts)):
        parent = ".".join(parts[:i])
        if parent not in sys.modules:
            sys.modules[parent] = types.ModuleType(parent)

    if mod_path not in sys.modules:
        sys.modules[mod_path] = types.ModuleType(mod_path)
    mod = sys.modules[mod_path]

    for name in _ALL_OPERATOR_NAMES:
        if not hasattr(mod, name):
            stub = type(name, (), {
                "__module__": mod_path,
                "execute": lambda self, context: None,
                # Common attributes that _submit_execute() introspects
                "task_id": "stub",
                "job": None,
                "main": None,
                "main_class": None,
                "main_jar": None,
                "dataproc_jars": None,
                "arguments": None,
                "pyfiles": None,
            })
            setattr(mod, name, stub)

    log.info(
        "[save-gcp-local] Installed mock Dataproc operator stubs in sys.modules "
        "so DAGs can import operator classes without the google provider package."
    )
    return mod


def _patch_fqcn_list(fqcn_list, fn, patched: list) -> None:
    """Patch a list of fully-qualified class names (module.ClassName) with fn."""
    for fqcn in fqcn_list:
        parts = fqcn.rsplit(".", 1)
        if len(parts) != 2:
            log.warning(
                "[save-gcp-local] Invalid FQCN '%s' in extra operators "
                "(expected 'module.ClassName'). Skipping.",
                fqcn,
            )
            continue
        mod_path, class_name = parts
        mod = sys.modules.get(mod_path)
        if mod is None:
            try:
                mod = importlib.import_module(mod_path)
            except ImportError:
                log.warning(
                    "[save-gcp-local] Cannot import '%s' for extra operator '%s'. "
                    "Is the package installed?",
                    mod_path, fqcn,
                )
                continue
        cls = getattr(mod, class_name, None)
        if cls is None:
            log.warning(
                "[save-gcp-local] Class '%s' not found in '%s'. Skipping.",
                class_name, mod_path,
            )
            continue
        cls.execute = fn
        patched.append(fqcn)


def apply_patches(config: Config = None) -> int:
    """Patch Dataproc operators. Returns number of operators patched.

    Idempotent: safe to call multiple times.

    When the google provider is not installed, installs mock stubs in
    sys.modules first so DAGs can still import and parse without error.
    """
    global _PATCHED
    config = config or load_config()

    if not config.enabled:
        log.info("[save-gcp-local] DPL_ENABLED is false — not patching.")
        return 0
    if _PATCHED:
        return 0

    try:
        from airflow.providers.google.cloud.operators import dataproc as dp
    except Exception as e:
        log.warning("[save-gcp-local] Could not import Dataproc operators: %s", e)
        log.info(
            "[save-gcp-local] Installing mock stubs so DAG files can import "
            "operator classes without the google provider package."
        )
        dp = _install_mock_stubs()

    runner = SparkRunner(config)
    submit = _submit_execute(runner)

    mapping = {
        "DataprocCreateClusterOperator": _noop_execute("CreateCluster"),
        "DataprocDeleteClusterOperator": _noop_execute("DeleteCluster"),
        "DataprocUpdateClusterOperator": _noop_execute("UpdateCluster"),
        "DataprocStartClusterOperator": _noop_execute("StartCluster"),
        "DataprocStopClusterOperator": _noop_execute("StopCluster"),
        "DataprocSubmitJobOperator": submit,
        "DataprocSubmitPySparkJobOperator": submit,
        "DataprocSubmitSparkJobOperator": submit,
        "DataprocSubmitSparkSqlJobOperator": submit,
        "DataprocSubmitHadoopJobOperator": submit,
        "DataprocCreateBatchOperator": submit,
        "DataprocInstantiateWorkflowTemplateOperator": _noop_execute("WorkflowTemplate"),
        "DataprocInstantiateInlineWorkflowTemplateOperator": _noop_execute("InlineWorkflow"),
        "DataprocSubmitHiveJobOperator": _hive_noop_execute("SubmitHiveJob"),
    }

    patched = []
    for name, fn in mapping.items():
        cls = getattr(dp, name, None)
        if cls is not None:
            cls.execute = fn
            patched.append(name)

    # Patch any extra custom operators declared via config (e.g. internal subclasses
    # in a bfdms.dpaas package that extend the base Dataproc operators).
    _patch_fqcn_list(config.extra_noop_operators, _noop_execute("CustomNoop"), patched)
    _patch_fqcn_list(config.extra_submit_operators, submit, patched)

    _PATCHED = True
    log.info(
        "[save-gcp-local] Patched %d Dataproc operators: %s",
        len(patched), ", ".join(patched) or "(none)",
    )
    return len(patched)
