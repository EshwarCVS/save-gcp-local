"""Command-line interface for save-gcp-local.

Subcommands:
  run          boot patching + run a DAG/task locally (or just set up the env)
  gen-data     populate test data via a chosen provider (none/sample/synthetic/BYO)
  pull-data    pull sample data from remote sources (GCS, S3, Azure, Hive, DB)
  connectors   list available data connectors
  patch        print which Dataproc operators would be patched (diagnostic)
  providers    list available data providers
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

from .config import Config, load_config

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("save_gcp_local.cli")


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
    log.info("save-gcp-local: patched %d Dataproc operators.", n)

    if args.task and args.dag:
        # Use sys.executable so patches, installed packages, and Python version
        # all match the current process. Plain "airflow" on PATH may be a
        # different Python environment entirely.
        from subprocess import call
        date = args.execution_date or "2024-01-01"
        log.info("Running task %s.%s for %s", args.dag, args.task, date)
        return call([sys.executable, "-m", "airflow", "tasks", "test", args.dag, args.task, date])
    if args.dag:
        from subprocess import call
        date = args.execution_date or "2024-01-01"
        log.info("Running full DAG %s for %s", args.dag, date)
        return call([sys.executable, "-m", "airflow", "dags", "test", args.dag, date])

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
    log.info("save-gcp-local: wrote test data -> %s (provider=%s)", dest, args.provider)
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


# -------------------------------------------------------------- pull-data
def cmd_pull_data(args) -> int:
    from .connectors import resolve_connector, get_connector, available as conn_available

    if args.config:
        return _pull_from_config(args)

    if not args.source:
        log.error("--source is required (or use --config for batch mode)")
        return 2

    connector = resolve_connector(args.source)
    if connector is None:
        if args.connector:
            try:
                connector = get_connector(args.connector)
            except KeyError:
                log.error("Unknown connector '%s'. Available: %s", args.connector, conn_available())
                return 2
        else:
            log.error(
                "Cannot determine connector for URI '%s'. "
                "Available connectors: %s. Use --connector to specify explicitly.",
                args.source, conn_available(),
            )
            return 2

    sample_size = args.sample_size
    dest = args.dest or _default_dest(args.source, args.data_dir)

    opts = {}
    if args.table:
        opts["table"] = args.table
    if args.query:
        opts["query"] = args.query
    if args.limit:
        opts["limit"] = args.limit

    try:
        result = connector.pull(args.source, dest, sample_size=sample_size,
                                seed=args.seed, **opts)
        log.info("save-gcp-local: data pulled -> %s (sample_size=%.2f)", result, sample_size)
        return 0
    except Exception as e:
        log.error("save-gcp-local: pull-data failed: %s", e)
        return 1


def _pull_from_config(args) -> int:
    from .pull_config import load_pull_config
    from .connectors import resolve_connector

    config = load_pull_config(args.config)
    override_sample = args.sample_size if args.sample_size != 1.0 else None
    failed = 0

    for spec in config.sources:
        sample_size = override_sample or spec.sample_size or config.default_sample_size
        connector = resolve_connector(spec.uri)
        if connector is None:
            log.error("No connector for source '%s' (uri=%s)", spec.name, spec.uri)
            failed += 1
            continue

        opts = {}
        if spec.table:
            opts["table"] = spec.table
        if spec.query:
            opts["query"] = spec.query
        if spec.limit:
            opts["limit"] = spec.limit

        try:
            connector.pull(spec.uri, spec.dest, sample_size=sample_size,
                           seed=config.seed, **opts)
            log.info("  [%s] -> %s (%.0f%% sample)", spec.name, spec.dest, sample_size * 100)
        except Exception as e:
            log.error("  [%s] FAILED: %s", spec.name, e)
            failed += 1

    if failed:
        log.error("save-gcp-local: %d source(s) failed", failed)
        return 1
    log.info("save-gcp-local: all %d sources pulled successfully", len(config.sources))
    return 0


def _default_dest(source: str, data_dir: str = None) -> str:
    """Derive a default destination path from the source URI."""
    import os
    data_dir = data_dir or os.environ.get("DPL_DATA_DIR", "./data")
    for prefix in ("gs://", "s3://", "s3a://", "abfs://", "abfss://", "wasbs://", "hive://"):
        if source.startswith(prefix):
            rest = source[len(prefix):]
            parts = rest.split("/", 1)
            key = parts[1] if len(parts) > 1 else parts[0]
            return os.path.join(data_dir, key)
    return os.path.join(data_dir, os.path.basename(source))


# -------------------------------------------------------------- connectors
def cmd_connectors(args) -> int:
    from .connectors import available_detail
    details = available_detail()
    if not details:
        log.info("No connectors available. Install optional dependencies:")
        log.info("  pip install 'save-gcp-local[gcs]'    # Google Cloud Storage")
        log.info("  pip install 'save-gcp-local[s3]'     # AWS S3")
        log.info("  pip install 'save-gcp-local[azure]'  # Azure Blob/ADLS")
        log.info("  pip install 'save-gcp-local[db]'     # JDBC databases")
        return 0
    log.info("Available data connectors:")
    for name, schemes in details.items():
        log.info("  %-10s  %s", name, ", ".join(schemes))
    return 0


# --------------------------------------------------------- install-airflow3
def cmd_install_airflow3(args) -> int:
    """Install a .pth file so Airflow 3.x applies patches before DAG parsing.

    Airflow 3.x parses DAGs BEFORE loading plugins, so the plugin approach
    registers patches too late.  This command drops a .pth file into your
    Python environment's site-packages.  Python processes it at startup,
    running save_gcp_local._early_patch before any Airflow code loads.

    After running this, set DPL_PATCH_EARLY=true in your Airflow environment
    (Airflow env vars, Kubernetes env, or Composer environment variable) to
    activate early patching.  The .pth file is a no-op unless that var is set,
    so it is safe to install globally in a shared environment.
    """
    import site
    candidates = getattr(site, "getsitepackages", lambda: [])()
    if not candidates:
        # Fallback for virtualenvs that only have getusersitepackages
        candidates = [site.getusersitepackages()]
    target_dir = candidates[0]

    import os
    pth_path = os.path.join(target_dir, "save_gcp_local_early.pth")
    with open(pth_path, "w") as f:
        f.write("import save_gcp_local._early_patch\n")

    log.info("Installed: %s", pth_path)
    log.info(
        "Next: set DPL_PATCH_EARLY=true in your Airflow environment "
        "(Composer env var / K8s env / airflow.cfg [core] env_var_prefix) "
        "so patching activates before DAG parsing."
    )
    log.info("Verify: restart the Airflow scheduler and look for '[save-gcp-local] Patched' in logs.")
    return 0


# -------------------------------------------------------------------- parse
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="save-gcp-local",
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

    # pull-data
    pd_ = sub.add_parser("pull-data",
                         help="Pull sample data from remote sources into local data dir.")
    pd_.add_argument("--source", help="Source URI (gs://, s3://, abfs://, hive://, postgresql://, etc.)")
    pd_.add_argument("--dest", help="Local destination path (default: derived from source)")
    pd_.add_argument("--sample-size", type=float, default=1.0,
                     help="Fraction of data to pull: 0.0-1.0 (e.g. 0.2 = 20%%). Default: 1.0 (all)")
    pd_.add_argument("--connector", help="Force a specific connector (gcs, s3, azure, hive, jdbc)")
    pd_.add_argument("--table", help="Table name (for JDBC/Hive connectors)")
    pd_.add_argument("--query", help="Custom SQL query (for JDBC/Hive connectors)")
    pd_.add_argument("--limit", type=int, help="Row limit (for JDBC/Hive connectors)")
    pd_.add_argument("--seed", type=int, default=42, help="Random seed for sampling")
    pd_.add_argument("--config", help="YAML/JSON config file defining multiple sources")
    pd_.add_argument("--data-dir", help="Base data directory (default: DPL_DATA_DIR or ./data)")
    pd_.set_defaults(func=cmd_pull_data)

    # connectors
    cn = sub.add_parser("connectors", help="List available data connectors.")
    cn.set_defaults(func=cmd_connectors)

    # patch
    pt = sub.add_parser("patch", help="Apply patches (diagnostic).")
    pt.add_argument("--disabled", action="store_true")
    pt.add_argument("--dry-run", action="store_true")
    pt.set_defaults(func=cmd_patch)

    # providers
    pr = sub.add_parser("providers", help="List data providers.")
    pr.set_defaults(func=cmd_providers)

    # install-airflow3
    ia = sub.add_parser(
        "install-airflow3",
        help=(
            "Install early-patch .pth file for Airflow 3.x. "
            "Airflow 3.x parses DAGs before loading plugins; this fixes that."
        ),
    )
    ia.set_defaults(func=cmd_install_airflow3)

    return p


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
