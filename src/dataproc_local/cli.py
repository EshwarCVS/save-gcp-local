"""Command-line interface for dataproc-local.

Subcommands:
  run        boot patching + run a DAG/task locally (or just set up the env)
  gen-data   populate test data via a chosen provider (none/sample/synthetic/BYO)
  patch      print which Dataproc operators would be patched (diagnostic)
  providers  list available data providers
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

from .config import Config, load_config

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("dataproc_local.cli")


def _apply_env_overrides(args) -> None:
    """Let CLI flags populate the env the rest of the library reads."""
    mapping = {
        "dags": None,  # handled separately
        "airflow_home": "AIRFLOW_HOME",
        "jobs_dir": "DPL_JOBS_DIR",
        "jobs_path": "DPL_JOBS_PATH",
        "data_dir": "DPL_DATA_DIR",
        "output_dir": "DPL_OUTPUT_DIR",
        "image": "DPL_DOCKER_IMAGE",
        "spark_master": "DPL_SPARK_MASTER",
        "runner": "DPL_RUNNER",
        "container_engine": "DPL_CONTAINER_ENGINE",
    }
    for attr, env in mapping.items():
        val = getattr(args, attr, None)
        if val and env:
            os.environ[env] = str(val)
    if getattr(args, "dry_run", False):
        os.environ["DPL_DRY_RUN"] = "true"
    if getattr(args, "disabled", False):
        os.environ["DPL_ENABLED"] = "false"


# --------------------------------------------------------------------- run
def cmd_run(args) -> int:
    _apply_env_overrides(args)

    # Make sure Airflow picks up the DAGs folder if provided.
    if args.dags:
        os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = os.path.abspath(args.dags)

    from .airflow_patch import apply_patches
    n = apply_patches(load_config())
    log.info("dataproc-local: patched %d Dataproc operators.", n)

    if args.task and args.dag:
        # Run a single task through Airflow's test path.
        from subprocess import call
        date = args.execution_date or "2024-01-01"
        log.info("Running task %s.%s for %s", args.dag, args.task, date)
        return call(["airflow", "tasks", "test", args.dag, args.task, date])
    if args.dag:
        from subprocess import call
        date = args.execution_date or "2024-01-01"
        log.info("Running full DAG %s for %s", args.dag, date)
        return call(["airflow", "dags", "test", args.dag, date])

    log.info(
        "Patching applied. Now start Airflow normally (e.g. `airflow standalone`) "
        "or trigger tasks from the UI — Dataproc steps run locally."
    )
    return 0


# ----------------------------------------------------------------- gen-data
def cmd_gen_data(args) -> int:
    from .providers import get_provider, available

    if args.provider not in available():
        log.error("Unknown provider '%s'. Available: %s", args.provider, available())
        return 2

    provider = get_provider(args.provider)
    opts = dict(
        pct=args.pct,
        rows=args.rows,
        seed=args.seed,
        jdbc=args.jdbc,
        table=args.table,
        limit=args.limit,
    )
    dest = provider.materialize(args.input or "", args.output, **opts)
    log.info("dataproc-local: wrote test data -> %s (provider=%s)", dest, args.provider)
    return 0


# ------------------------------------------------------------------- patch
def cmd_patch(args) -> int:
    _apply_env_overrides(args)
    from .airflow_patch import apply_patches
    n = apply_patches(load_config())
    log.info("Patched %d operators.", n)
    return 0


def cmd_providers(args) -> int:
    from .providers import available
    log.info("Available data providers: %s", ", ".join(available()))
    return 0


# -------------------------------------------------------------------- parse
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="dataproc-local",
        description="Run Airflow DAGs locally; execute Dataproc/Spark jobs in local Docker.",
    )
    sub = p.add_subparsers(dest="command", required=True)

    # run
    r = sub.add_parser("run", help="Apply patching and optionally run a DAG/task.")
    r.add_argument("--dags", help="Path to DAGs folder")
    r.add_argument("--airflow-home", help="AIRFLOW_HOME to use")
    r.add_argument("--dag", help="DAG id to run")
    r.add_argument("--task", help="Task id to run (requires --dag)")
    r.add_argument("--execution-date", help="Execution date YYYY-MM-DD")
    r.add_argument("--jobs-dir", help="Primary host dir with job files -> /jobs")
    r.add_argument("--jobs-path", help="Extra search roots for job files (comma-separated). "
                                       "Use when jobs live in the Airflow repo, subfolders, or other repos/JARs.")
    r.add_argument("--data-dir", help="Host dir with input data -> /data")
    r.add_argument("--output-dir", help="Host dir for output -> /output")
    r.add_argument("--image", help="Spark docker image")
    r.add_argument("--spark-master", help="Spark master (default local[*])")
    r.add_argument("--runner", choices=["docker", "local"], help="Execution runner")
    r.add_argument("--container-engine", choices=["auto", "docker", "podman"],
                   help="Container CLI for the docker runner (default: auto-detect)")
    r.add_argument("--dry-run", action="store_true", help="Print spark-submit, don't execute")
    r.add_argument("--disabled", action="store_true", help="Do not patch (passthrough to GCP)")
    r.set_defaults(func=cmd_run)

    # gen-data
    g = sub.add_parser("gen-data", help="Populate test data via a provider.")
    g.add_argument("--provider", default="sample",
                   help="none | sample | synthetic | <custom> (default: sample)")
    g.add_argument("--input", help="Source file (csv/parquet/json)")
    g.add_argument("--output", required=True, help="Destination path")
    g.add_argument("--pct", type=float, default=1.0, help="Sample percentage")
    g.add_argument("--rows", type=int, default=100000, help="Rows for synthetic")
    g.add_argument("--seed", type=int, default=42)
    g.add_argument("--jdbc", help="SQLAlchemy URL for DB source")
    g.add_argument("--table", help="Table name with --jdbc")
    g.add_argument("--limit", type=int, help="Row cap when loading")
    g.set_defaults(func=cmd_gen_data)

    # patch
    pt = sub.add_parser("patch", help="Apply patches (diagnostic).")
    pt.add_argument("--disabled", action="store_true")
    pt.add_argument("--dry-run", action="store_true")
    pt.set_defaults(func=cmd_patch)

    # providers
    pr = sub.add_parser("providers", help="List data providers.")
    pr.set_defaults(func=cmd_providers)

    return p


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
