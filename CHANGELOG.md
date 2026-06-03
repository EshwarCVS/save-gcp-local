# Changelog

All notable changes to this project are documented here.
This project adheres to [Semantic Versioning](https://semver.org/).

## [0.1.0] - Unreleased

### Added
- Intercept GCP Dataproc operators in local Airflow; run Spark jobs locally instead of creating a cluster.
- Cluster lifecycle operators (create/delete/update/start/stop, workflow templates) become no-ops.
- Job-submit operators (modern `DataprocSubmitJobOperator`, `DataprocCreateBatchOperator`, and legacy PySpark/Spark/SparkSQL/Hadoop operators) run via local `spark-submit`.
- **Docker and Podman** support with auto-detection; plus a `local` runner that uses a host `spark-submit`.
- Job resolver: finds job files across multiple roots (Airflow repo, subfolders, JARs, separate repos); handles relative/absolute/remote paths.
- Pluggable test-data providers: `none`, `sample` (subset of real data), `synthetic` (shape-preserving generated data), plus a registration API for custom providers.
- Two entry points: a `dataproc-local` CLI and an auto-loading Airflow plugin.
- `DPL_ENABLED=false` master switch to pass through to real GCP unchanged.
- Dependency-light core; heavy deps behind `[data]`, `[db]`, `[airflow]`, `[all]` extras.
