"""Airflow integration: monkey-patch Dataproc operators to run locally.

Importing this module (which the Airflow plugin and the CLI both do) replaces
`.execute()` on the Dataproc operators so cluster lifecycle becomes a no-op and
job submission is routed to the local SparkRunner.

Generic: it patches whatever Dataproc operators exist in the installed provider
version, and reads job specs from the attributes used across operator versions.
"""

from __future__ import annotations

import logging

from .config import Config, load_config
from .runner import SparkRunner

log = logging.getLogger("dataproc_local.airflow_patch")

_PATCHED = False


def _noop_execute(label: str):
    def execute(self, context):  # noqa: ANN001
        log.info(
            "[save-gcp-local] %s on task '%s' -> SKIPPED (no GCP cluster, no cost).",
            label, getattr(self, "task_id", "?"),
        )
        return {"dataproc_local": "skipped", "operator": label}
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


def apply_patches(config: Config = None) -> int:
    """Patch Dataproc operators. Returns number of operators patched.

    Idempotent: safe to call multiple times.
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
    except Exception as e:  # provider missing / not in Airflow
        log.warning("[save-gcp-local] Could not import Dataproc operators: %s", e)
        return 0

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
    }

    patched = []
    for name, fn in mapping.items():
        cls = getattr(dp, name, None)
        if cls is not None:
            cls.execute = fn
            patched.append(name)

    _PATCHED = True
    log.info(
        "[save-gcp-local] Patched %d Dataproc operators: %s",
        len(patched), ", ".join(patched) or "(none)",
    )
    return len(patched)
