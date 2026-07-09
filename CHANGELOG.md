# Changelog

All notable changes to this project are documented here.
This project adheres to [Semantic Versioning](https://semver.org/).

## [0.4.0] - 2026-07-09

### Added
- **Full local service stack** — `docker-compose.override.yml` now includes Hive Metastore, OpenSearch, and SQL Server (Azure DB equivalent) alongside PostgreSQL. All services start with `astro dev start` — zero manual setup.
- **Spark connector JARs pre-installed** — Dockerfile now downloads PostgreSQL, SQL Server (MSSQL), and MySQL JDBC connector JARs into `$SPARK_HOME/jars/` so Spark jobs can talk to all local services out of the box.
- **Hive, OpenSearch, and MSSQL connection defaults** — `connections.py` auto-creates `hive_default`, `opensearch_default`, and `mssql_default` Airflow connections pointing to local services. DAGs using these connections work without manual setup.
- **Airflow Variables auto-import** — new `variables.py` module sets `AIRFLOW_VAR_*` env vars from a JSON file (`DPL_VARIABLES_FILE`) or `DPL_VAR_*` env vars. DAGs calling `Variable.get()` work locally without manual Variable creation in the UI.
- **`DPL_SPARK_CONF`** — comma-separated `key=value` pairs passed as `--conf` flags to `spark-submit`. Pre-configured with Hive metastore URI and catalog implementation in scaffolded projects.
- **`DPL_CONNECTOR_JARS`** — additional connector JARs outside `$SPARK_HOME/jars/` auto-included in `--jars` flag.
- **`--engine` flag for `init-astro`** — choose `docker` or `podman` container engine at scaffolding time. Sets `DPL_CONTAINER_ENGINE` in `.env`.
- **`include/local_variables.json`** — scaffolded with default Variables (Hive metastore URI, OpenSearch host, MSSQL host, GCP project, environment=local).
- Plugin now auto-loads Variables on Airflow startup alongside connections.

## [0.3.0] - 2026-07-09

### Added
- **Astro CLI integration** — `save-gcp-local init-astro` scaffolds an Astronomer Astro project for local Dataproc development in one command. Generates Dockerfile (Java + Spark), Airflow plugin, `.env` config, `docker-compose.override.yml` with local services, and connection overrides.
- **Connection patching** — automatically overrides Airflow connections (`google_cloud_default`, database connections) to point to local services at Airflow startup. Supports JSON config files and `DPL_CONN_*` environment variables.
- **Data connectors** — pluggable connectors for pulling sample data from remote sources into the local data directory for testing. Connectors auto-dispatch based on URI scheme.
  - **GCS connector** (`gs://`) — pull files/prefixes from Google Cloud Storage with tabular sampling
  - **S3 connector** (`s3://`, `s3a://`) — pull from AWS S3 with tabular sampling
  - **Azure Blob connector** (`abfs://`, `abfss://`, `wasbs://`) — pull from Azure Blob Storage / ADLS
  - **Hive connector** (`hive://`) — pull from Hive metastore tables via PyHive or SQLAlchemy
  - **JDBC connector** (`postgresql://`, `mysql://`, `jdbc:`) — pull from any SQLAlchemy-supported database
- **`init-astro` CLI command** — one-command Astro project scaffolding
- **`pull-data` CLI command** — pull sample data from any supported source with `--sample-size` (0.0-1.0 fraction)
- **Batch pull mode** — define multiple data sources in a YAML/JSON config file and pull them all at once with `--config`
- **`connectors` CLI command** — list available connectors and their URI schemes
- **Custom connectors** — subclass `Connector`, implement `pull()`, and register with `@register`
- New optional dependency groups: `[gcs]`, `[s3]`, `[azure]`, `[hive]`, `[yaml]`, `[connectors]`

## [0.2.1] - 2026-06-05

### Added
- Fixed issues 
- Docker image tag
- CLI subprocess / wrong Python	Replaced call(["airflow", ...]) 
- Missing google provider = silent failure	apply_patches() now installs mock stub classes in sys.modules when the google provider is absent, so DAGs can import operator classes without ModuleNotFoundError	airflow_patch.py
- Custom operator subclasses invisible	Added DPL_EXTRA_NOOP_OPERATORS and DPL_EXTRA_SUBMIT_OPERATORS config vars (comma-sep FQCNs) — patches internal subclasses like bfdms.dpaas.BFDMSDataprocCreateClusterOperator	config.py, airflow_patch.py
- Hive operator not supported	Added DataprocSubmitHiveJobOperator as a smart no-op that logs the skipped HQL	airflow_patch.py
- Container engine auto-detect fragile
- Documentation

## [0.2.0] - 2026-06-05

### Added
- gh-pages

## [0.1.0] - 2026-06-04

### Added
- Intercept GCP Dataproc operators in local Airflow; run Spark jobs locally instead of creating a cluster.
- Cluster lifecycle operators (create/delete/update/start/stop, workflow templates) become no-ops.
- Job-submit operators (modern `DataprocSubmitJobOperator`, `DataprocCreateBatchOperator`, and legacy PySpark/Spark/SparkSQL/Hadoop operators) run via local `spark-submit`.
- **Docker and Podman** support with auto-detection; plus a `local` runner that uses a host `spark-submit`.
- Job resolver: finds job files across multiple roots (Airflow repo, subfolders, JARs, separate repos); handles relative/absolute/remote paths.
- Pluggable test-data providers: `none`, `sample` (subset of real data), `synthetic` (shape-preserving generated data), plus a registration API for custom providers.
- Two entry points: a `save-gcp-local` CLI and an auto-loading Airflow plugin.
- `DPL_ENABLED=false` master switch to pass through to real GCP unchanged.
- Dependency-light core; heavy deps behind `[data]`, `[db]`, `[airflow]`, `[all]` extras.
