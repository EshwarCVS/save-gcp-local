# save-gcp-local

**Stop paying for Dataproc clusters just to test your Spark jobs.** Run them locally in Docker or Podman instead — same code, zero cloud cost, no DAG changes.

[![CI](https://github.com/EshwarCVS/save-gcp-local/actions/workflows/ci.yml/badge.svg)](https://github.com/EshwarCVS/save-gcp-local/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/save-gcp-local)](https://pypi.org/project/save-gcp-local/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://www.python.org)

> **[Interactive Demo](https://claude.ai/code/artifact/beca3443-ec2b-4ae8-9fdb-c315adf8266a)** — see how it works, setup steps, cost comparison, and data connectors in one page.

---

## Why this exists

Testing Spark jobs on GCP Dataproc is **slow and expensive**. Every small code change means:

1. Trigger the DAG
2. Wait for a cluster to spin up (1–3 min)
3. Run the job on full data (often 30–40 min)
4. Tear the cluster down
5. Find a bug -> repeat — **and pay for all of it**

The cluster minutes add up fast, especially across a whole team iterating all day.

**save-gcp-local removes the cluster entirely.** It intercepts the Dataproc steps in your local Airflow and runs the *same* Spark job in a local container. You iterate in seconds for free, then do **one** real Dataproc run at the end to confirm scale.

> **Can you run Dataproc itself locally?** No — Dataproc is GCP infrastructure. But your *job* is plain Apache Spark, which has a built-in local mode. This tool no-ops the cluster steps and runs your job locally. That is the whole trick, and it is enough to save the money.

## What you save

| Step | On Dataproc | Locally |
|------|------------|---------|
| Cluster create | 1–3 min + $ | skipped, $0 |
| Job run | 30–40 min + $ | seconds–min, $0 |
| Cluster delete | ~1 min + $ | skipped, $0 |
| **Per iteration** | **~40 min + cluster cost** | **~minutes, free** |

## Key features

- **Zero DAG edits** — works by patching Dataproc operators at runtime
- **Astro CLI integration** — `save-gcp-local init-astro` scaffolds your Astro project in one command
- **Generic** — any Dataproc operator, PySpark or Scala/Java JARs, any project layout
- **Docker *or* Podman** (or a local `spark-submit`) — auto-detected, daemon health checked
- **Jobs anywhere** — in the Airflow repo, a subfolder, a JAR, or a separate repo
- **Data connectors** — pull sample data from GCS, S3, Azure Blob, Hive, PostgreSQL, MySQL, and more
- **Sample size control** — `--sample-size 0.2` pulls 20% of your data for fast local testing
- **Connection patching** — GCP and database connections auto-redirect to local services
- **Test data your way** — none / real-data sample / synthetic / your own provider
- **Custom operator subclasses** — patch internal wrappers via `DPL_EXTRA_*_OPERATORS`
- **Airflow 2.x and 3.x** — plugin for 2.x, early-patch `.pth` for 3.x
- **Missing google provider** — installs mock stubs so DAGs still import and parse
- **One switch to go back to GCP** — `DPL_ENABLED=false`

## Install

```bash
pip install "save-gcp-local[all]"        # from PyPI (when published)
# or from source:
git clone https://github.com/EshwarCVS/save-gcp-local
cd save-gcp-local && pip install -e ".[all]"
```

## 60-second start

```bash
# 1. Point at your test data (jobs inside the Airflow repo are auto-found)
export DPL_DATA_DIR=./data

# 2. (optional) pull sample data from your real sources — pick what fits
#    Pull 20% of a GCS file:
save-gcp-local pull-data --source gs://my-bucket/events.csv --sample-size 0.2
#    Pull 10% of a database table:
save-gcp-local pull-data --source postgresql://host/db --table orders --sample-size 0.1 --dest ./data/orders.csv
#    Or generate test data locally:
save-gcp-local gen-data --provider sample    --input prod.csv --output ./data/events.csv --pct 1
save-gcp-local gen-data --provider synthetic --input prod.csv --output ./data/events.csv --rows 200000

# 3. run your DAG locally — Dataproc steps run in a container
save-gcp-local run --dags ./dags --dag my_pipeline --execution-date 2024-06-01
```

Prefer the UI? Use Astro CLI (see below) or drop a one-liner into `$AIRFLOW_HOME/plugins/` and boot Airflow normally — see **[QUICKSTART.md](QUICKSTART.md)**.

## Astro CLI integration (recommended for teams)

If your team uses [Astronomer's Astro CLI](https://www.astronomer.io/docs/astro/cli/overview), one command sets everything up:

```bash
# 1. Start with a normal Astro project
astro dev init        # if you don't have one yet

# 2. Scaffold save-gcp-local into it
pip install save-gcp-local
save-gcp-local init-astro .

# 3. Put your jobs and test data in place
cp my_spark_job.py include/jobs/
cp test_data.csv   include/data/

# 4. Start Airflow — Dataproc tasks run locally, zero cloud cost
astro dev start
```

### What `init-astro` generates

| File | Purpose |
|------|---------|
| `Dockerfile` (appended) | Installs Java, Apache Spark, and save-gcp-local into the Airflow image |
| `plugins/save_gcp_local_plugin.py` | Auto-patches Dataproc operators + sets up local connections on Airflow boot |
| `.env` | All `DPL_*` configuration (runner=local, Spark paths, data dirs) |
| `docker-compose.override.yml` | Local PostgreSQL (and optionally MySQL, fake-GCS) for database connections |
| `include/local_connections.json` | Connection overrides — `google_cloud_default` becomes a mock, DB connections point local |
| `include/jobs/`, `include/data/`, `include/output/` | Mount directories for your Spark code, test data, and results |

### How it works with Astro

```
astro dev start
    |
    +-- builds your Dockerfile (now has Java + Spark + save-gcp-local)
    +-- starts Airflow webserver, scheduler, triggerer, postgres
    +-- starts local-postgres (from docker-compose.override.yml)
    |
    +-- Airflow boots:
        +-- plugin loads -> patches all Dataproc operators
        +-- connections set up -> google_cloud_default = mock, dbs = local
        |
        +-- You trigger a DAG in the UI:
            CreateCluster -> SKIPPED (no-op, $0)
            SubmitJob     -> spark-submit --master local[*] /jobs/your_job.py
            DeleteCluster -> SKIPPED (no-op, $0)
```

### Connection patching

DAGs reference Airflow connections like `google_cloud_default` or database connections. Normally these need real cloud credentials. With save-gcp-local:

- **`google_cloud_default`** — becomes a mock (Dataproc operators are patched anyway)
- **Database connections** — redirect to the local PostgreSQL spun up by docker-compose.override.yml
- **Custom connections** — define in `include/local_connections.json` or via `DPL_CONN_*` env vars

```bash
# Add a connection override via env var
export DPL_CONN_MY_DATABASE='{"conn_type":"postgres","host":"local-postgres","port":5432,"login":"localuser","password":"localpass","schema":"local_warehouse"}'
```

### Switch back to real GCP

```bash
# In .env, change one line:
DPL_ENABLED=false
# Restart: astro dev restart
```

Your DAGs were never modified. Production behavior is identical.

## Data connectors — pull sample data from anywhere

Instead of copying full datasets, pull just what you need for testing:

```bash
# Pull 20% of data from GCS
save-gcp-local pull-data --source gs://bucket/events/ --sample-size 0.2

# Pull 10% from S3
save-gcp-local pull-data --source s3://bucket/data.parquet --sample-size 0.1

# Pull 5% from Azure ADLS
save-gcp-local pull-data --source abfss://container@account.dfs.core.windows.net/data.csv --sample-size 0.05

# Pull from a Hive table
save-gcp-local pull-data --source hive://hive-host:10000/analytics/events --sample-size 0.2

# Pull from PostgreSQL / MySQL / any SQLAlchemy-supported database
save-gcp-local pull-data --source postgresql://user:pass@host/db --table orders --sample-size 0.2
```

### Batch mode — pull multiple sources at once

Define all your data sources in a YAML config file:

```yaml
# dpl-sources.yaml
default_sample_size: 0.2    # 20% of each source

sources:
  events:
    uri: gs://my-bucket/events/2024-06-01.csv
    dest: ./data/events/2024-06-01.csv
  users:
    uri: postgresql://host/db
    table: users
    dest: ./data/users.parquet
    sample_size: 0.1          # override: just 10%
```

```bash
save-gcp-local pull-data --config dpl-sources.yaml
```

### Available connectors

| Connector | URI schemes | Install extra |
|-----------|-------------|---------------|
| GCS | `gs://` | `pip install 'save-gcp-local[gcs]'` |
| S3 | `s3://`, `s3a://` | `pip install 'save-gcp-local[s3]'` |
| Azure | `abfs://`, `abfss://`, `wasbs://` | `pip install 'save-gcp-local[azure]'` |
| Hive | `hive://` | `pip install 'save-gcp-local[hive]'` |
| JDBC | `postgresql://`, `mysql://`, `jdbc:` | `pip install 'save-gcp-local[db]'` |

```bash
save-gcp-local connectors    # list what's available in your install
```

## Documentation

- **[Interactive Demo](https://claude.ai/code/artifact/beca3443-ec2b-4ae8-9fdb-c315adf8266a)** — visual walkthrough with live examples
- **[QUICKSTART.md](QUICKSTART.md)** — 5-minute setup
- **[SETUP.md](SETUP.md)** — full guide: install options, config, both entry points, test-data strategies, troubleshooting
- **[CICD.md](CICD.md)** — CI/CD pipeline, release process, branch protection
- **[CONTRIBUTING.md](CONTRIBUTING.md)** — dev setup, tests, how to add a data provider
- **[Docs site](https://eshwarcvs.github.io/save-gcp-local)** — full documentation website

## How it works

```
                   On GCP (expensive)                    With save-gcp-local (free)
                   ─────────────────                     ─────────────────────────
  DAG triggers     Create Dataproc cluster (1-3 min, $)  SKIPPED (no-op)
       |           Submit Spark job (30-40 min, $)       spark-submit local[*] (seconds)
       v           Delete cluster (1 min, $)             SKIPPED (no-op)
                   DB connections -> cloud databases      DB connections -> local Postgres
                   ──────────────────────────────         ─────────────────────────────────
                   Total: ~40 min + cluster cost          Total: seconds, $0
```

Cluster lifecycle operators become no-ops. Job-submit operators run your Spark code locally via `spark-submit --master local[*]`. Database connections auto-redirect to local services.

## Supported operators

Cluster lifecycle (no-op): `DataprocCreateClusterOperator`, `DataprocDeleteClusterOperator`, `DataprocUpdate/Start/StopClusterOperator`, workflow-template operators, `DataprocSubmitHiveJobOperator`.

Job submission (runs locally): `DataprocSubmitJobOperator`, `DataprocCreateBatchOperator`, and legacy `DataprocSubmitPySparkJobOperator` / `SparkJobOperator` / `SparkSqlJobOperator` / `HadoopJobOperator`.

Custom operator subclasses (e.g. internal wrappers that extend the base operators) can be patched via `DPL_EXTRA_NOOP_OPERATORS` and `DPL_EXTRA_SUBMIT_OPERATORS` — see SETUP.md §7.

## Limitations (be honest with your team)

- Local Spark is a **single machine** — validate *logic* locally, *scale* on GCP once.
- Absolute row counts / huge-shuffle behavior will not match production.
- If a job hardcodes `gs://`/BigQuery paths *inside the code* (not as an argument), parameterize the input so it can point at `/data`.

## License

MIT — see [LICENSE](LICENSE).
