# QUICKSTART (5 minutes)

> Can't run "Dataproc" locally — it's GCP infrastructure. But your **Spark job** runs locally fine in Spark local mode. This library skips the cluster step and runs the job. Use it to validate logic fast; do one real GCP run at the end for scale.

## 1. Install

```bash
pip install "dataproc-local[all]"     # or: pip install -e ".[all]" from source
```

## 2. Point it at your stuff

```bash
export DPL_JOBS_DIR=/path/to/your/spark-repo
export DPL_DATA_DIR=/path/to/test-data
export DPL_OUTPUT_DIR=/path/to/output
```

## 3. (Optional) Make test data

```bash
# subset of real data:
dataproc-local gen-data --provider sample --input prod.csv --output ./data/events.csv --pct 1
# OR generated data matching real shape:
dataproc-local gen-data --provider synthetic --input prod.csv --output ./data/events.csv --rows 200000
```

## 4. Run

**CLI:**
```bash
dataproc-local run --dags ./dags --dag my_pipeline --execution-date 2024-06-01
```

**Or Airflow plugin** — drop this in `$AIRFLOW_HOME/plugins/dataproc_local_plugin.py`:
```python
from dataproc_local.airflow_plugin import *  # noqa
```
then boot Airflow and use the UI as usual.

## 5. Confirm it ran locally

Look for:
```
[dataproc-local] CreateCluster ... -> SKIPPED (no GCP cluster, no cost).
[dataproc-local] ... -> running locally (runner=docker)
```

## Turn off (back to real GCP)
```bash
export DPL_ENABLED=false
```

Full details: see **SETUP.md**.
```

| I want to… | Do this |
|------------|---------|
| Test logic fast, free | Run locally with sampled data |
| Use real values | `--provider sample --pct 1` |
| Generate more data | `--provider synthetic --rows N` |
| Stage data myself | `--provider none` |
| See the command only | `--dry-run` |
| Go back to GCP | `DPL_ENABLED=false` |
