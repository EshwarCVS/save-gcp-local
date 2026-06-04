# save-gcp-local — Developer Setup & Usage Guide

A practical, step-by-step guide for installing the library and running your Dataproc/Spark jobs locally, adapted to your own setup.

---

## 0. First: can you actually run Dataproc locally?

Read this before anything else — it removes the confusion.

**You cannot download or run "Dataproc" on your laptop.** Dataproc is Google's managed infrastructure: it provisions VMs, runs `spark-submit` on them, and tears them down. There is no local Dataproc.

**But you don't need to.** Your job is plain **Apache Spark** (PySpark or Scala). Dataproc is just the thing that *launches* it. Spark has a built-in **local mode** (`--master local[*]`) that runs the entire job in a single JVM on your machine — and code that runs in local mode is the same code that runs on a cluster.

So the model is:

```
On GCP:    [Dataproc creates cluster] -> [spark-submit your job] -> [delete cluster]
Locally:   [skip — can't run Dataproc] -> [spark-submit your job] -> [skip]
                                              ^^^^^^^^^^^^^^^^^^^
                                              this is identical
```

This library skips the parts that can only exist on GCP (cluster lifecycle) and runs the part that matters (your Spark job) locally in Docker or via a local `spark-submit`.

**What you CAN validate locally:** transformation logic, schema, SQL, UDFs, joins, filters, business rules, code bugs, dependency issues.

**What you CANNOT fully validate locally:** exact row counts at production scale, behavior of very large shuffles, partition skew, cluster-sizing issues. Local is one machine.

**The recommended workflow:** iterate locally (seconds–minutes, $0) until logic is correct, then do ONE real GCP run at the end to confirm scale. That's where the 30–40 minutes per iteration is saved.

---

## 1. Prerequisites

### Required for the docker runner (default)

| Requirement | Minimum version | How to check |
|-------------|-----------------|--------------|
| Docker **or** Podman | Docker 20+ / Podman 4+ | `docker info` or `podman info` |
| Java (JDK or JRE) | 11+ (Spark 3.x requires Java 11 or 17) | `java -version` |
| Python | 3.8+ | `python --version` |

> **Container not starting?** The library verifies the daemon responds before launching (`docker info` / `podman info`). If `DPL_CONTAINER_ENGINE=auto` picks the wrong engine, override it: `export DPL_CONTAINER_ENGINE=docker`.

### Required for the local runner (`DPL_RUNNER=local`)

| Requirement | Notes |
|-------------|-------|
| Apache Spark | `spark-submit` must be on PATH, or set `DPL_SPARK_SUBMIT_CMD=/path/to/spark-submit` |
| Java 11+ | Same as above |

### Required for JAR/Scala jobs

| Requirement | Notes |
|-------------|-------|
| Pre-built JAR | Your Scala/Java job must already be compiled. Build with `mvn package` or `sbt assembly` before running. |
| `DPL_JOBS_DIR` or `DPL_JOBS_PATH` | Point at the directory containing your compiled JAR. |

> **No Maven/Gradle at runtime.** The library does not compile code — it runs `spark-submit` against an existing JAR. Build the JAR first.

### Airflow version

| Airflow version | Support | Notes |
|-----------------|---------|-------|
| 2.5 – 2.10 | Full | Plugin approach works; DAGs parsed after plugins load |
| 3.0 – 3.1 | Full (with setup) | DAGs parsed before plugins; requires `install-airflow3` (see §5) |
| < 2.5 | Partial | `airflow.utils.dates.days_ago` available; older operator names may differ |

---

## 2. Install

Pick whichever fits your team.

### Option A — from PyPI (once published)

```bash
pip install save-gcp-local            # core only, zero heavy deps
pip install "save-gcp-local[data]"    # + sample/synthetic data providers (pandas/numpy)
pip install "save-gcp-local[db]"      # + database sources (SQLAlchemy)
pip install "save-gcp-local[all]"     # everything
```

### Option B — from source (clone + editable)

```bash
git clone https://github.com/EshwarCVS/save-gcp-local
cd save-gcp-local
pip install -e ".[all]"               # editable: your edits take effect immediately
```

### Option C — from the built wheel (offline / internal mirror)

```bash
pip install save_gcp_local-0.2.0-py3-none-any.whl
# or with extras:
pip install "save_gcp_local-0.2.0-py3-none-any.whl[all]"
```

Verify:

```bash
save-gcp-local providers
# core only      -> none
# with [data]    -> none, sample, synthetic
```

---

## 3. Tell the library where YOUR things live

Everything is driven by environment variables (so the CLI and the Airflow plugin behave identically). Set these once per shell, or put them in a `.env` / your Airflow startup script.

### Where do the Spark job files live?

They can be anywhere — and the library handles all of these without you forcing one layout:

- **Inside the Airflow repo** (jobs next to DAGs, or in `jobs/`, `spark/`, `include/`, `plugins/`, `src/`). When running under Airflow, these are auto-discovered: the resolver searches the DAGs folder, the repo root, and `AIRFLOW_HOME` automatically.
- **A JAR** (Scala/Java) on disk, in the repo or elsewhere.
- **A separate repo** somewhere else on the machine.
- **A `gs://` / `s3://` reference** in the operator — matched locally by its key path or basename.

You do not need to copy jobs into one folder. Point the resolver at one or more **search roots** and it finds the file wherever it is, mounting that directory into the container automatically:

```bash
# Jobs live inside the Airflow repo — usually nothing to set; auto-discovered.
# To be explicit, or to add extra roots (e.g. a separate Scala repo / JAR dir):
export DPL_JOBS_PATH="/path/to/airflow-repo,/path/to/scala-repo/target,/path/to/jars"
```

The resolver tries each root, checking common subfolders (`jobs/`, `spark/`, `include/`, `dags/`, `plugins/`, `src/`), and uses the first match. Relative paths, absolute paths, JARs, and remote URIs all resolve.

### The core variables

```bash
# Required-ish: point at your job code and data
export DPL_JOBS_DIR=/abs/path/to/your/spark-repo     # mounted to /jobs in the container
export DPL_DATA_DIR=/abs/path/to/test-data           # mounted to /data
export DPL_OUTPUT_DIR=/abs/path/to/output            # mounted to /output

# Choose how jobs run
export DPL_RUNNER=docker                             # docker (default) | local
export DPL_DOCKER_IMAGE=apache/spark:3.5.0           # any Spark image you trust
export DPL_SPARK_MASTER="local[*]"                   # use all cores

# Master switch
export DPL_ENABLED=true                              # false = passthrough to real GCP
```

Full list of options is in section 9.

---

## 4. Run it — two entry points

### Entry point 1: the CLI

Good for quick, scripted, one-off runs.

> **Important:** The CLI applies patches in the current Python process, then spawns Airflow as a subprocess using `sys.executable -m airflow`. Both processes use the same Python interpreter and installed packages. The Airflow plugin **must be installed** (the `save_gcp_local` package on `PYTHONPATH`) for patches to reach the Airflow subprocess — the CLI sets patches in memory, not the child process. For full in-process patching use the plugin approach (Entry point 2) or Airflow 3.x early patching (§5).

```bash
# Just apply the interception, then you start Airflow yourself:
save-gcp-local run --jobs-dir ./spark-repo --data-dir ./data
airflow standalone        # trigger tasks in the UI; Dataproc steps run locally

# Or run a whole DAG in one shot:
save-gcp-local run \
  --dags ./dags \
  --jobs-dir ./spark-repo \
  --data-dir ./data \
  --dag my_pipeline \
  --execution-date 2024-06-01

# Or a single task (fast debugging of one step):
save-gcp-local run \
  --dags ./dags \
  --dag my_pipeline \
  --task spark_transform \
  --execution-date 2024-06-01

# See exactly what spark-submit WOULD run, without running it:
save-gcp-local run --dag my_pipeline --task spark_transform --dry-run
```

### Entry point 2: the Airflow plugin (auto-load) — Airflow 2.x

Good for matching your real flow: boot Airflow, use the UI as normal. Works fully in **Airflow 2.x** where plugins load before DAG parsing.

1. Drop a one-line file into your Airflow plugins folder:

```python
# $AIRFLOW_HOME/plugins/save_gcp_local_plugin.py
from save_gcp_local.airflow_plugin import *   # noqa
```

2. Export the `DPL_*` vars (section 3).
3. Start Airflow however you normally do.

On startup the logs show:

```
[save-gcp-local] Patched N Dataproc operators: DataprocCreateClusterOperator, DataprocSubmitJobOperator, ...
```

4. Trigger tasks in the UI exactly as before. Variables and Connections resolve normally; only the cluster step is replaced.

For Airflow 3.x, use the early-patch approach in §5 instead.

---

## 5. Airflow 3.x — early patching

Airflow 3.x changed plugin loading order: **DAGs are parsed before plugins load**. This means the plugin-based approach registers patches too late — the DAG has already imported operator classes before patches are applied.

The fix is a Python `.pth` file that runs patching code at interpreter startup, before any Airflow code:

```bash
# One-time setup: installs a .pth file in site-packages
save-gcp-local install-airflow3

# Then set this env var in your Airflow environment (Composer, K8s, airflow.cfg, etc.)
export DPL_PATCH_EARLY=true
export DPL_ENABLED=true
export DPL_DOCKER_IMAGE=apache/spark:3.5.0
# ... other DPL_* vars as needed
```

The `.pth` file is a no-op unless `DPL_PATCH_EARLY=true` is set, so it is safe to install in a shared environment.

**Verify it works:** Restart the Airflow scheduler and look for this in logs:

```
[save-gcp-local] Patched N Dataproc operators: ...
```

If it still shows 0 operators patched, check:
- `DPL_PATCH_EARLY=true` is visible to the scheduler process (not just the web server)
- `save_gcp_local_early.pth` exists in your Python's site-packages (`python -c "import site; print(site.getsitepackages())"`)

### Airflow 3.x import changes

Airflow 3.x removed some commonly used imports. Update any DAG code that uses:

| Old (Airflow 2.x) | New (Airflow 3.x) |
|-------------------|-------------------|
| `from airflow.utils.dates import days_ago` | `from datetime import datetime, timedelta` |
| `from airflow.contrib.operators.*` | `from airflow.providers.*` |
| `from airflow.operators.python import PythonOperator` | `from airflow.providers.standard.operators.python import PythonOperator` |

These are user DAG changes, not library changes — `save-gcp-local` does not use any of the deprecated imports internally.

---

## 6. JAR and Scala/Java jobs

The library fully supports Scala and Java jobs submitted as JARs. No special configuration is needed beyond pointing at the JAR file.

### Pre-built JAR (most common)

```python
# In your DAG — DataprocSubmitJobOperator with a spark_job spec
SUBMIT_JOB = DataprocSubmitJobOperator(
    task_id="transform",
    job={
        "spark_job": {
            "main_class": "com.example.TransformJob",
            "jar_file_uris": ["gs://my-bucket/jars/etl-assembly-1.0.jar"],
            "args": ["--date", "{{ ds }}", "--env", "prod"],
        }
    },
    ...
)
```

Locally, point the resolver at the compiled JAR:

```bash
export DPL_JOBS_PATH="/path/to/scala-repo/target/scala-2.12"
# The resolver finds etl-assembly-1.0.jar by basename, mounts its directory,
# and runs: spark-submit --class com.example.TransformJob /jobs/ext0/etl-assembly-1.0.jar ...
```

### Multiple JARs (main + dependencies)

```python
"spark_job": {
    "main_class": "com.example.Main",
    "jar_file_uris": [
        "gs://bucket/jars/main.jar",         # submitted first
        "gs://bucket/jars/dep-lib.jar",      # passed as --jars
    ],
    "args": [...],
}
```

The resolver handles each JAR independently. The first URI is the main JAR; the rest become `--jars`.

### Legacy SparkJobOperator (Airflow 2.x)

```python
DataprocSubmitSparkJobOperator(
    task_id="run_job",
    main_class="com.example.Job",
    dataproc_jars=["gs://bucket/jars/job.jar"],
    arguments=["--date", "2024-06-01"],
    ...
)
```

Both modern (`DataprocSubmitJobOperator` with a dict) and legacy operators are supported.

### Recommended Docker image for JAR jobs

The official `apache/spark:3.5.0` image has `spark-submit` at `/opt/spark/bin/spark-submit`. The image's custom entrypoint does not add this directory to `PATH`. Either:

**Option A — override the spark-submit path (simplest):**
```bash
export DPL_SPARK_SUBMIT_CMD=/opt/spark/bin/spark-submit
```

**Option B — use a custom image with PATH fixed (recommended for teams):**
```dockerfile
FROM apache/spark:3.5.0
ENV PATH="/opt/spark/bin:${PATH}"
ENTRYPOINT []
```

```bash
docker build -t my-spark:3.5.0 .
export DPL_DOCKER_IMAGE=my-spark:3.5.0
```

**Option C — override the container entrypoint:**
```bash
export DPL_DOCKER_ENTRYPOINT=/bin/bash
export DPL_SPARK_SUBMIT_CMD=/opt/spark/bin/spark-submit
```

---

## 7. Custom operator subclasses

Production DAGs often use internal wrappers around the base Dataproc operators. For example:

```python
# bfdms/dpaas/operators.py (internal package)
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

class BFDMSDataprocSubmitJobOperator(DataprocSubmitJobOperator):
    """Internal wrapper that adds org-specific defaults."""
    ...
```

The library only patches operators in the `airflow.providers.google.cloud.operators.dataproc` module by default. Subclasses in other packages are invisible to the standard patch.

### Fix: declare extra operators via config

```bash
# Comma-separated list of FQCNs (module.ClassName)
export DPL_EXTRA_NOOP_OPERATORS=\
  bfdms.dpaas.operators.BFDMSDataprocCreateClusterOperator,\
  bfdms.dpaas.operators.BFDMSDataprocDeleteClusterOperator

export DPL_EXTRA_SUBMIT_OPERATORS=\
  bfdms.dpaas.operators.BFDMSDataprocSubmitJobOperator
```

The library will import each module, find the class, and patch its `execute()` method — exactly the same as the built-in operators.

### Fix: patch subclasses that inherit automatically

If your subclass does not override `execute()`, patching the parent class is sufficient — Python's MRO means the patched parent method is inherited. Declare the extra operators only if the subclass overrides `execute()` itself.

---

## 8. Provide test data — your choice (this is optional)

If your jobs read local files you've already staged, skip this entirely (`--provider none`). Otherwise pick a strategy:

```bash
# (a) Do nothing — you stage /data yourself
save-gcp-local gen-data --provider none --input mydata.csv --output ./data/events.csv

# (b) SAMPLE: a subset of REAL data, exact values preserved
save-gcp-local gen-data --provider sample \
  --input prod_export.csv --output ./data/events.csv --pct 1

# (c) SYNTHETIC: learn the shape of real data, generate MORE rows (no real values copied)
save-gcp-local gen-data --provider synthetic \
  --input prod_export.csv --output ./data/events.csv --rows 500000

# From a database table instead of a file:
save-gcp-local gen-data --provider sample \
  --jdbc postgresql://user:pass@host:5432/db --table events \
  --pct 2 --output ./data/events.csv
```

### Matching cloud paths

Jobs usually read `gs://bucket/events/2024-06-01.csv`. The runner rewrites that to `/data/events/2024-06-01.csv` inside the container. So write your test data to the matching subpath:

```bash
save-gcp-local gen-data --provider sample \
  --input prod.csv \
  --output "$DPL_DATA_DIR/events/2024-06-01.csv" --pct 1
```

### Bring your own provider

If neither sample nor synthetic fits (e.g. you call an internal anonymization API):

```python
# myteam_provider.py
from save_gcp_local.providers import register, DataProvider

@register
class MyProvider(DataProvider):
    name = "myteam"
    def materialize(self, source, dest, **opts):
        # produce data at `dest`, then:
        return dest
```

Import it before running, then `--provider myteam`.

---

## 9. Verify it's actually running locally

Run a single Dataproc task and watch the logs:

```bash
save-gcp-local run --dag my_pipeline --task spark_transform --execution-date 2024-06-01
```

You should see:

```
[save-gcp-local] CreateCluster on task 'create_cluster' -> SKIPPED (no GCP cluster, no cost).
[save-gcp-local] spark_transform (pyspark) -> running locally (runner=docker)
[save-gcp-local] docker run --rm ... spark-submit --master local[*] /jobs/transform.py ...
[spark] ... your job output ...
[save-gcp-local] spark_transform completed.
```

If you see `Patched 0 operators`, jump to section 11.

---

## 10. Turning it off (run against real GCP again)

```bash
export DPL_ENABLED=false
```

or delete the plugin file. **Your DAGs were never modified**, so production behavior is identical to before the library existed.

---

## 11. All configuration options

| Env var | CLI flag | Default | Meaning |
|---------|----------|---------|---------|
| `DPL_ENABLED` | `--disabled` (inverts) | `true` | Master on/off |
| `DPL_RUNNER` | `--runner` | `docker` | `docker` or `local` (host spark-submit) |
| `DPL_CONTAINER_ENGINE` | `--container-engine` | `auto` | `auto` / `docker` / `podman` — which container CLI (auto checks daemon health) |
| `DPL_DOCKER_IMAGE` | `--image` | `apache/spark:3.5.0` | Spark container image |
| `DPL_DOCKER_ENTRYPOINT` | — | *(not set)* | Override the container entrypoint (e.g. `/bin/bash`) |
| `DPL_SPARK_SUBMIT_CMD` | — | `spark-submit` | Path to spark-submit inside the container (e.g. `/opt/spark/bin/spark-submit`) |
| `DPL_SPARK_MASTER` | `--spark-master` | `local[*]` | Spark master URL |
| `DPL_JOBS_DIR` | `--jobs-dir` | `./jobs` | Primary host dir → `/jobs` |
| `DPL_JOBS_PATH` | `--jobs-path` | — | Extra search roots for job files (comma list) |
| `DPL_DATA_DIR` | `--data-dir` | `./data` | Host dir → `/data` |
| `DPL_OUTPUT_DIR` | `--output-dir` | `./output` | Host dir → `/output` |
| `DPL_EXTRA_PACKAGES` | — | — | `--packages` (comma list) |
| `DPL_EXTRA_JARS` | — | — | extra `--jars` (comma list) |
| `DPL_PATH_PREFIXES` | — | `gs://,s3://,s3a://,abfs://,hdfs://` | Remote prefixes rewritten to `/data` |
| `DPL_DOCKER_NETWORK` | — | `bridge` | Docker network |
| `DPL_DOCKER_MEMORY` | — | — | e.g. `8g` |
| `DPL_DRY_RUN` | `--dry-run` | `false` | Print spark-submit, don't execute |
| `DPL_EXTRA_NOOP_OPERATORS` | — | — | Comma-sep FQCNs of custom operators to no-op |
| `DPL_EXTRA_SUBMIT_OPERATORS` | — | — | Comma-sep FQCNs of custom operators to run locally |
| `DPL_PATCH_EARLY` | — | `false` | Set `true` for Airflow 3.x early patching via .pth |

CLI subcommands:

```
save-gcp-local run                # apply patches + optionally run a DAG/task
save-gcp-local gen-data           # populate test data via a provider
save-gcp-local patch              # apply patches only (diagnostic)
save-gcp-local providers          # list available data providers
save-gcp-local install-airflow3   # install early-patch .pth file for Airflow 3.x
```

---

## 12. Troubleshooting

### `Patched 0 Dataproc operators`

- `apache-airflow-providers-google` not installed → library now installs mock stubs so DAGs can still import; check that `DPL_ENABLED` is not `false`.
- Running on Airflow 3.x without early patching → run `save-gcp-local install-airflow3` and set `DPL_PATCH_EARLY=true`.
- `DPL_ENABLED=false` — set it to `true`.

### `exec: spark-submit: not found` (exit 127) inside the container

The official `apache/spark:3.5.0` image does not add `/opt/spark/bin` to PATH. Fix:

```bash
export DPL_SPARK_SUBMIT_CMD=/opt/spark/bin/spark-submit
```

Or build a thin wrapper image:

```dockerfile
FROM apache/spark:3.5.0
ENV PATH="/opt/spark/bin:${PATH}"
ENTRYPOINT []
```

### `docker: command not found` / `Cannot connect to the Docker daemon`

Docker/Podman not installed or daemon not running. The library checks connectivity before launching (`docker info`). Either:

- Start Docker: `docker desktop start` or `sudo systemctl start docker`
- Or for Podman: `podman machine start`
- Or override to avoid auto-detection: `export DPL_CONTAINER_ENGINE=docker`
- Or switch to local runner: `export DPL_RUNNER=local` (needs Spark installed locally)

### `Podman detected but not responding` / stale SSH socket

This happens after a VM crash. Run `podman machine stop && podman machine start`, or set `DPL_CONTAINER_ENGINE=docker` to bypass auto-detection.

### `ClassNotFoundException` (Scala/Java JAR jobs)

- `main_class` string does not match the compiled class name — check with `jar tf your.jar | grep "\.class"`.
- The JAR is not in the search path — set `DPL_JOBS_PATH=/path/to/jars`.
- The JAR is not on the `DPL_DOCKER_IMAGE` image — mount it via `DPL_JOBS_DIR` or `DPL_JOBS_PATH`.

### `ModuleNotFoundError: No module named 'airflow.providers.google'` in DAG

The library installs mock stubs in `sys.modules` so this should not propagate past `apply_patches()`. If you still see it, `apply_patches()` was called too late (Airflow 3.x plugin timing issue). Use the early-patch approach (§5).

### Custom operators not being patched

Subclasses in other packages require `DPL_EXTRA_NOOP_OPERATORS` or `DPL_EXTRA_SUBMIT_OPERATORS`. See §7.

### Hive tasks skipped / not executed

`DataprocSubmitHiveJobOperator` is patched as a no-op locally — HQL cannot run against the embedded Derby metastore. The skipped HQL is logged so you can inspect it. To actually validate HQL, run a separate Hive / Spark SQL session against the same data.

### Job runs but can't find its input file

The `gs://` path didn't map to a staged file. Check what path the job actually reads (often an Airflow Variable or `--input` arg), then stage data at the matching `/data/...` subpath (section 8).

### Counts/sums look wrong vs production

Expected. You're on sampled/synthetic data on a single machine. Validate logic, not absolute totals. Do a final full run on GCP.

### Job reads BigQuery/GCS directly in code

The runner only rewrites paths it can see in the operator's job spec. If the path is hardcoded inside the job, parameterize it (take an input arg) so it can point at `/data` locally.

### Wrong Python / wrong Airflow version used

The CLI uses `sys.executable -m airflow` to invoke Airflow, guaranteeing the same Python and installed packages. If you still see the wrong version, the installed `airflow` package in that venv may be different from what `which airflow` returns. Verify with `python -m airflow version`.

---

## 13. Quick mental model

- **Dataproc** = GCP's job launcher. Can't be local. The library no-ops it.
- **Your Spark job** = plain Spark. Runs locally in Spark local mode. The library runs it.
- **Test data** = your choice: none / sample / synthetic / your own.
- **Iterate locally for logic, validate on GCP once for scale.**
