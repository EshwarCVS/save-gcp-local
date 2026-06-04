# QUICKSTART (5 minutes)

> Can't run "Dataproc" locally — it's GCP infrastructure. But your **Spark job** runs locally fine in Spark local mode. This library skips the cluster step and runs the job. Use it to validate logic fast; do one real GCP run at the end for scale.

## Prerequisites

| What | Minimum | Notes |
|------|---------|-------|
| Python | 3.8+ | — |
| Docker **or** Podman | Docker 20+ / Podman 4+ | Must be running (`docker info` / `podman info`) |
| Java | 11 or 17 | Required by Spark inside the container; usually satisfied by the image |
| Airflow | 2.5+ or 3.x | 3.x requires one extra step — see step 4b |

For JAR/Scala jobs: build your JAR first (`mvn package` / `sbt assembly`). The library runs `spark-submit`; it does not compile code.

---

## 1. Install

```bash
pip install "save-gcp-local[all]"     # from PyPI (when published)
# or from source:
git clone https://github.com/EshwarCVS/save-gcp-local
cd save-gcp-local && pip install -e ".[all]"
```

---

## 2. Point it at your stuff

```bash
export DPL_JOBS_DIR=/path/to/your/spark-repo   # or JAR directory
export DPL_DATA_DIR=/path/to/test-data
export DPL_OUTPUT_DIR=/path/to/output
```

Jobs inside the Airflow repo (`jobs/`, `spark/`, `include/`, `dags/`) are auto-discovered — nothing to set.

**Container engine** (auto-detected from what's running, but you can be explicit):
```bash
export DPL_CONTAINER_ENGINE=docker    # or podman
```

**Official spark image note:** `apache/spark:3.5.0` puts `spark-submit` at `/opt/spark/bin/spark-submit`, not on PATH. Set:
```bash
export DPL_SPARK_SUBMIT_CMD=/opt/spark/bin/spark-submit
```
Or see SETUP.md §6 for a one-line Dockerfile fix.

---

## 3. (Optional) Make test data

```bash
# subset of real data:
save-gcp-local gen-data --provider sample --input prod.csv --output ./data/events.csv --pct 1
# OR generated data matching real shape:
save-gcp-local gen-data --provider synthetic --input prod.csv --output ./data/events.csv --rows 200000
```

---

## 4. Run

### 4a. CLI (Airflow 2.x and 3.x)

```bash
save-gcp-local run --dags ./dags --dag my_pipeline --execution-date 2024-06-01
```

> The CLI patches operators in the current process then spawns Airflow via `python -m airflow`. The `save_gcp_local` package must be importable by the Airflow process (same venv). For reliable in-process patching use the plugin approach (4b).

### 4b. Airflow plugin — Airflow 2.x

Drop this in `$AIRFLOW_HOME/plugins/save_gcp_local_plugin.py`:
```python
from save_gcp_local.airflow_plugin import *  # noqa
```
Then boot Airflow and use the UI as usual.

### 4b. Airflow 3.x — early patching

In Airflow 3.x, DAGs are parsed **before** plugins load. Run this once:
```bash
save-gcp-local install-airflow3
```
Then set these in your Airflow environment (Composer env var, K8s env, etc.):
```bash
export DPL_PATCH_EARLY=true
export DPL_ENABLED=true
```
The library will patch operators at Python startup, before any DAG is parsed. Restart the scheduler and look for `[save-gcp-local] Patched N operators` in logs.

---

## 5. Confirm it ran locally

Look for:
```
[save-gcp-local] CreateCluster ... -> SKIPPED (no GCP cluster, no cost).
[save-gcp-local] ... -> running locally (runner=docker)
```

---

## Turn off (back to real GCP)
```bash
export DPL_ENABLED=false
```

---

## Quick reference

| I want to… | Do this |
|------------|---------|
| Test PySpark logic fast, free | Run locally with sampled data |
| Test a JAR/Scala job | Set `DPL_JOBS_PATH=/path/to/jars`, use `DPL_SPARK_SUBMIT_CMD` if needed |
| Use real values | `--provider sample --pct 1` |
| Generate more rows | `--provider synthetic --rows N` |
| Stage data myself | `--provider none` |
| See the command only | `--dry-run` |
| Patch custom operators | `DPL_EXTRA_NOOP_OPERATORS=my.pkg.Op` or `DPL_EXTRA_SUBMIT_OPERATORS=my.pkg.Op` |
| Use with Airflow 3.x | `save-gcp-local install-airflow3` + `DPL_PATCH_EARLY=true` |
| Go back to GCP | `DPL_ENABLED=false` |

Full details: see **SETUP.md**.
