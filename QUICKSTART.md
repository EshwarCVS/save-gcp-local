# QUICKSTART (5 minutes)

> Your Spark job is plain Apache Spark. Dataproc is just the GCP launcher. This library skips the launcher and runs your job locally in a Docker/Podman container. Validate logic fast and free; do one real GCP run at the end for scale.

---

## Prerequisites

| What | Minimum | How to check |
|------|---------|--------------|
| Python | 3.8+ | `python --version` |
| Docker **or** Podman | Docker 20+ / Podman 4+ | `docker info` or `podman info` |
| Airflow | 2.5+ or 3.x | `python -m airflow version` |

Docker or Podman must be **running** (not just installed). The library checks daemon health before launching.

---

## 1. Install

```bash
pip install "save-gcp-local[all]"     # from PyPI (when published)

# or from source:
git clone https://github.com/EshwarCVS/save-gcp-local
cd save-gcp-local && pip install -e ".[all]"
```

Verify:
```bash
save-gcp-local providers
# Expected output: none, sample, synthetic
```

---

## 2. End-to-end example (copy-paste)

This example creates a tiny PySpark job, generates test data, and runs the DAG locally. Everything executes in a Docker (or Podman) container — no GCP needed.

```bash
# Create a workspace
mkdir -p /tmp/sgl-demo/{dags,jobs,data,output}
cd /tmp/sgl-demo

# Create a simple PySpark job
cat > jobs/word_count.py << 'PYEOF'
import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WordCount").getOrCreate()
df = spark.read.csv(sys.argv[1], header=True)
print(f"Row count: {df.count()}")
df.show(5)
spark.stop()
PYEOF

# Create test data
cat > data/sample.csv << 'EOF'
id,name,value
1,Alice,100
2,Bob,200
3,Charlie,300
EOF

# Create an Airflow DAG that uses Dataproc operators
cat > dags/demo_pipeline.py << 'DAGEOF'
from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)

with DAG("demo_pipeline", start_date=datetime(2024, 1, 1), schedule_interval=None) as dag:
    create = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id="my-project",
        region="us-central1",
        cluster_name="my-cluster",
        cluster_config={},
    )
    submit = DataprocSubmitJobOperator(
        task_id="run_spark",
        project_id="my-project",
        region="us-central1",
        job={
            "pyspark_job": {
                "main_python_file_uri": "gs://my-bucket/jobs/word_count.py",
                "args": ["gs://my-bucket/data/sample.csv"],
            },
            "placement": {"cluster_name": "my-cluster"},
        },
    )
    delete = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id="my-project",
        region="us-central1",
        cluster_name="my-cluster",
    )
    create >> submit >> delete
DAGEOF

# Set environment
export DPL_ENABLED=true
export DPL_JOBS_DIR=$PWD/jobs
export DPL_DATA_DIR=$PWD/data
export DPL_OUTPUT_DIR=$PWD/output

# Run the DAG — Dataproc steps run in a local container
save-gcp-local run --dags ./dags --dag demo_pipeline --execution-date 2024-06-01
```

What happens:
- `DataprocCreateClusterOperator` is **skipped** (no GCP cluster, no cost)
- `DataprocSubmitJobOperator` runs `word_count.py` inside a Docker/Podman container via `spark-submit --master local[*]`
- `DataprocDeleteClusterOperator` is **skipped**

You should see output like:
```
[save-gcp-local] CreateCluster on task 'create_cluster' -> SKIPPED (no GCP cluster, no cost).
[save-gcp-local] run_spark (pyspark) -> running locally (runner=docker)
[spark] Row count: 3
[save-gcp-local] run_spark completed.
[save-gcp-local] DeleteCluster on task 'delete_cluster' -> SKIPPED (no GCP cluster, no cost).
```

---

## 3. How jobs run in containers

Every Spark job executes inside a container (Docker or Podman). The library:

1. **Auto-detects** the container engine (Docker or Podman) by checking which daemon responds to `docker info` / `podman info`
2. **Mounts** your directories into the container:
   - `DPL_JOBS_DIR` -> `/jobs` (your Spark code)
   - `DPL_DATA_DIR` -> `/data` (test data)
   - `DPL_OUTPUT_DIR` -> `/output` (results)
3. **Rewrites** remote paths (`gs://`, `s3://`, etc.) to local `/data/` paths
4. **Runs** `spark-submit --master local[*]` inside the container

```
Your machine                          Container (Docker/Podman)
-----------                          -------------------------
./jobs/word_count.py        --->     /jobs/word_count.py
./data/sample.csv           --->     /data/sample.csv
./output/                   --->     /output/
                                     spark-submit --master local[*]
                                       /jobs/word_count.py /data/sample.csv
```

### Override the container engine

```bash
export DPL_CONTAINER_ENGINE=docker    # or podman, or auto (default)
```

### Use a different Spark image

```bash
export DPL_DOCKER_IMAGE=apache/spark:3.5.0    # default
export DPL_DOCKER_IMAGE=bitnami/spark:3.5     # alternative
export DPL_DOCKER_IMAGE=my-org/spark:latest   # your own
```

### Skip containers entirely (local spark-submit)

```bash
export DPL_RUNNER=local
# Requires spark-submit on PATH or:
export DPL_SPARK_SUBMIT_CMD=/path/to/spark-submit
```

---

## 4. Configure for your project

```bash
# Required: point at your job code and data
export DPL_JOBS_DIR=/path/to/your/spark-repo
export DPL_DATA_DIR=/path/to/test-data
export DPL_OUTPUT_DIR=/path/to/output

# Jobs inside the Airflow repo (jobs/, spark/, include/, dags/) are auto-discovered
# Add extra search roots for jobs in other repos or JAR directories:
export DPL_JOBS_PATH="/path/to/scala-repo/target,/path/to/jars"
```

**Official `apache/spark:3.5.0` image note:** `spark-submit` is at `/opt/spark/bin/spark-submit`, not on PATH. Fix:
```bash
export DPL_SPARK_SUBMIT_CMD=/opt/spark/bin/spark-submit
```

---

## 5. Generate test data (optional)

```bash
# Subset of real data (exact values preserved):
save-gcp-local gen-data --provider sample --input prod.csv --output ./data/events.csv --pct 1

# Synthetic data matching real shape (no real values copied):
save-gcp-local gen-data --provider synthetic --input prod.csv --output ./data/events.csv --rows 200000
```

---

## 6. Airflow integration

### Airflow 2.x — plugin (recommended)

Drop this file in `$AIRFLOW_HOME/plugins/save_gcp_local_plugin.py`:
```python
from save_gcp_local.airflow_plugin import *  # noqa
```
Then boot Airflow normally. Dataproc tasks run locally.

### Airflow 3.x — early patching

DAGs are parsed before plugins in 3.x, so an extra step is needed:
```bash
save-gcp-local install-airflow3       # one-time setup
export DPL_PATCH_EARLY=true           # add to your Airflow env
export DPL_ENABLED=true
```
Restart the scheduler. Look for `[save-gcp-local] Patched N operators` in logs.

---

## 7. Turn off (back to real GCP)

```bash
export DPL_ENABLED=false
```

Your DAGs are never modified. Production behavior is identical to before the library existed.

---

## Quick reference

| I want to... | Do this |
|--------------|---------|
| Test PySpark locally | Set `DPL_JOBS_DIR`, `DPL_DATA_DIR`, run with CLI or plugin |
| Test a JAR/Scala job | Set `DPL_JOBS_PATH=/path/to/jars` |
| Use Podman instead of Docker | `export DPL_CONTAINER_ENGINE=podman` |
| Skip containers entirely | `export DPL_RUNNER=local` (needs local Spark) |
| See the command without running | `--dry-run` |
| Patch custom operators | `DPL_EXTRA_NOOP_OPERATORS=my.pkg.Op` or `DPL_EXTRA_SUBMIT_OPERATORS=my.pkg.Op` |
| Use with Airflow 3.x | `save-gcp-local install-airflow3` + `DPL_PATCH_EARLY=true` |
| Go back to GCP | `DPL_ENABLED=false` |

Full details: **[SETUP.md](SETUP.md)**
