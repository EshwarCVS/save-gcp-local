# Changelog

All notable changes to this project are documented here.
This project adheres to [Semantic Versioning](https://semver.org/).

## [0.3.0] - 2026-07-09

### Added
- **Data connectors** — pluggable connectors for pulling sample data from remote sources into the local data directory for testing. Connectors auto-dispatch based on URI scheme.
  - **GCS connector** (`gs://`) — pull files/prefixes from Google Cloud Storage with tabular sampling
  - **S3 connector** (`s3://`, `s3a://`) — pull from AWS S3 with tabular sampling
  - **Azure Blob connector** (`abfs://`, `abfss://`, `wasbs://`) — pull from Azure Blob Storage / ADLS
  - **Hive connector** (`hive://`) — pull from Hive metastore tables via PyHive or SQLAlchemy
  - **JDBC connector** (`postgresql://`, `mysql://`, `jdbc:`) — pull from any SQLAlchemy-supported database
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
