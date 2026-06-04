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

Depending on which runner you choose:

| Runner | Needs |
|--------|-------|
| `docker` (default) | Docker **or** Podman installed & running (auto-detected; Podman is CLI-compatible) |
| `local` | A local Apache Spark install (`spark-submit` on PATH) + a JDK |

Plus:
- Python 3.8+
- Your DAGs folder (from git or a Composer download)
- Your Spark job files (the separate repo you already have locally)
- `apache-airflow` + `apache-airflow-providers-google` if you use the Airflow integration (you likely already have your own pinned versions)

---

## 2. Install

Pick whichever fits your team.

### Option A — from PyPI (once published)

```bash
pip install dataproc-local            # core only, zero heavy deps
pip install "dataproc-local[data]"    # + sample/synthetic data providers (pandas/numpy)
pip install "dataproc-local[db]"      # + database sources (SQLAlchemy)
pip install "dataproc-local[all]"     # everything
```

### Option B — from source (clone + editable)

```bash
git clone https://github.com/EshwarCVS/save-gcp-local
cd save-gcp-local
pip install -e ".[all]"               # editable: your edits take effect immediately
```

### Option C — from the built wheel (offline / internal mirror)

```bash
pip install dataproc_local-0.1.0-py3-none-any.whl
# or with extras:
pip install "dataproc_local-0.1.0-py3-none-any.whl[all]"
```

Verify:

```bash
dataproc-local providers
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
export DPL_DOCKER_IMAGE=apache/spark:3.5.0-python3   # any Spark image you trust
export DPL_SPARK_MASTER="local[*]"                   # use all cores

# Master switch
export DPL_ENABLED=true                              # false = passthrough to real GCP
```

Full list of options is in section 8.

---

## 4. Run it — two entry points

### Entry point 1: the CLI

Good for quick, scripted, one-off runs.

```bash
# Just apply the interception, then you start Airflow yourself:
dataproc-local run --jobs-dir ./spark-repo --data-dir ./data
airflow standalone        # trigger tasks in the UI; Dataproc steps run locally

# Or run a whole DAG in one shot:
dataproc-local run \
  --dags ./dags \
  --jobs-dir ./spark-repo \
  --data-dir ./data \
  --dag my_pipeline \
  --execution-date 2024-06-01

# Or a single task (fast debugging of one step):
dataproc-local run \
  --dags ./dags \
  --dag my_pipeline \
  --task spark_transform \
  --execution-date 2024-06-01

# See exactly what spark-submit WOULD run, without running it:
dataproc-local run --dag my_pipeline --task spark_transform --dry-run
```

### Entry point 2: the Airflow plugin (auto-load)

Good for matching your real flow: boot Airflow, use the UI as normal.

1. Drop a one-line file into your Airflow plugins folder:

```python
# $AIRFLOW_HOME/plugins/dataproc_local_plugin.py
from dataproc_local.airflow_plugin import *   # noqa
```

2. Export the `DPL_*` vars (section 3).
3. Start Airflow however you normally do.

On startup the logs show:

```
[dataproc-local] Patched N Dataproc operators: DataprocCreateClusterOperator, DataprocSubmitJobOperator, ...
```

4. Trigger tasks in the UI exactly as before. Variables and Connections resolve normally; only the cluster step is replaced.

---

## 5. Provide test data — your choice (this is optional)

If your jobs read local files you've already staged, skip this entirely (`--provider none`). Otherwise pick a strategy:

```bash
# (a) Do nothing — you stage /data yourself
dataproc-local gen-data --provider none --input mydata.csv --output ./data/events.csv

# (b) SAMPLE: a subset of REAL data, exact values preserved
dataproc-local gen-data --provider sample \
  --input prod_export.csv --output ./data/events.csv --pct 1

# (c) SYNTHETIC: learn the shape of real data, generate MORE rows (no real values copied)
dataproc-local gen-data --provider synthetic \
  --input prod_export.csv --output ./data/events.csv --rows 500000

# From a database table instead of a file:
dataproc-local gen-data --provider sample \
  --jdbc postgresql://user:pass@host:5432/db --table events \
  --pct 2 --output ./data/events.csv
```

### Matching cloud paths

Jobs usually read `gs://bucket/events/2024-06-01.csv`. The runner rewrites that to `/data/events/2024-06-01.csv` inside the container. So write your test data to the matching subpath:

```bash
dataproc-local gen-data --provider sample \
  --input prod.csv \
  --output "$DPL_DATA_DIR/events/2024-06-01.csv" --pct 1
```

### Bring your own provider

If neither sample nor synthetic fits (e.g. you call an internal anonymization API):

```python
# myteam_provider.py
from dataproc_local.providers import register, DataProvider

@register
class MyProvider(DataProvider):
    name = "myteam"
    def materialize(self, source, dest, **opts):
        # produce data at `dest`, then:
        return dest
```

Import it before running, then `--provider myteam`.

---

## 6. Verify it's actually running locally

Run a single Dataproc task and watch the logs:

```bash
dataproc-local run --dag my_pipeline --task spark_transform --execution-date 2024-06-01
```

You should see:

```
[dataproc-local] CreateCluster on task 'create_cluster' -> SKIPPED (no GCP cluster, no cost).
[dataproc-local] spark_transform (pyspark) -> running locally (runner=docker)
[dataproc-local] docker run --rm ... spark-submit --master local[*] /jobs/transform.py ...
[spark] ... your job output ...
[dataproc-local] spark_transform completed.
```

If you see `Patched 0 operators`, jump to section 9.

---

## 7. Turning it off (run against real GCP again)

```bash
export DPL_ENABLED=false
```

or delete the plugin file. **Your DAGs were never modified**, so production behavior is identical to before the library existed.

---

## 8. All configuration options

| Env var | CLI flag | Default | Meaning |
|---------|----------|---------|---------|
| `DPL_ENABLED` | `--disabled` (inverts) | `true` | Master on/off |
| `DPL_RUNNER` | `--runner` | `docker` | `docker` or `local` (host spark-submit) |
| `DPL_CONTAINER_ENGINE` | `--container-engine` | `auto` | `auto` / `docker` / `podman` — which container CLI to use for the docker runner |
| `DPL_DOCKER_IMAGE` | `--image` | `apache/spark:3.5.0-python3` | Spark image |
| `DPL_SPARK_MASTER` | `--spark-master` | `local[*]` | Spark master URL |
| `DPL_JOBS_DIR` | `--jobs-dir` | `./jobs` | Primary host dir → `/jobs` |
| `DPL_JOBS_PATH` | `--jobs-path` | — | Extra search roots for job files (comma list); jobs in the Airflow repo, subfolders, other repos, or JARs |
| `DPL_DATA_DIR` | `--data-dir` | `./data` | Host dir → `/data` |
| `DPL_OUTPUT_DIR` | `--output-dir` | `./output` | Host dir → `/output` |
| `DPL_EXTRA_PACKAGES` | — | — | `--packages` (comma list) |
| `DPL_EXTRA_JARS` | — | — | extra `--jars` (comma list) |
| `DPL_PATH_PREFIXES` | — | `gs://,s3://,s3a://,abfs://,hdfs://` | Remote prefixes rewritten to `/data` |
| `DPL_DOCKER_NETWORK` | — | `bridge` | Docker network |
| `DPL_DOCKER_MEMORY` | — | — | e.g. `8g` |
| `DPL_DRY_RUN` | `--dry-run` | `false` | Print spark-submit, don't execute |

CLI subcommands:

```
dataproc-local run         # apply patches + optionally run a DAG/task
dataproc-local gen-data    # populate test data via a provider
dataproc-local patch       # apply patches only (diagnostic)
dataproc-local providers   # list available data providers
```

---

## 9. Troubleshooting

**`Patched 0 Dataproc operators`**
- `apache-airflow-providers-google` not installed in the same environment.
- `DPL_ENABLED=false`. Set it `true`.
- Running outside Airflow with no provider present (expected for `--dry-run` smoke tests).

**`docker: command not found` / `Cannot connect to the Docker daemon`**
- Docker not installed or not running. Either start Docker, or switch to `DPL_RUNNER=local` if you have a local Spark install.

**Job runs but can't find its input file**
- The `gs://` path didn't map to a staged file. Check what path the job actually reads (often an Airflow Variable or `--input` arg), then stage data at the matching `/data/...` subpath (section 5).

**`ClassNotFoundException` (Scala)**
- `main_class` doesn't match the JAR, or the compiled JAR isn't in `DPL_JOBS_DIR`.

**Counts/sums look wrong vs production**
- Expected. You're on sampled/synthetic data on a single machine. Validate logic, not absolute totals. Do a final full run on GCP.

**Job reads BigQuery/GCS directly in code**
- The runner only rewrites paths it can see in the operator's job spec. If the path is hardcoded inside the job, parameterize it (take an input arg) so it can point at `/data` locally.

---

## 10. Quick mental model

- **Dataproc** = GCP's job launcher. Can't be local. The library no-ops it.
- **Your Spark job** = plain Spark. Runs locally in Spark local mode. The library runs it.
- **Test data** = your choice: none / sample / synthetic / your own.
- **Iterate locally for logic, validate on GCP once for scale.**
