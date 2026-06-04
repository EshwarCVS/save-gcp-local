# Contributing to save-gcp-local

Thanks for helping make local Dataproc testing cheaper for everyone. This guide gets you from clone to passing tests in a couple of minutes.

## Dev setup

```bash
git clone https://github.com/EshwarCVS/save-gcp-local
cd save-gcp-local
python -m venv .venv && source .venv/bin/activate
pip install -e ".[all,dev]"
```

## Run the tests

```bash
pytest -q
```

The core test suite has **no external dependencies** — no Airflow, Docker, or cloud needed. It tests path rewriting, job-spec parsing, the job resolver, container-engine selection, and the data providers, all with `DPL_DRY_RUN=true`.

## Project layout

```
src/dataproc_local/
  config.py          # all settings, resolved from env vars
  runner.py          # generic Spark runner (Docker/Podman/local). No Airflow.
  resolver.py        # finds job files across many roots (repo, subfolders, JARs)
  airflow_patch.py   # monkey-patches Dataproc operators
  airflow_plugin.py  # Airflow plugin entry point (auto-loads the patch)
  cli.py             # `dataproc-local` command
  providers/         # pluggable test-data providers
    __init__.py      # registry + NoneProvider
    tabular.py       # SampleProvider, SyntheticProvider (need the [data] extra)
tests/               # dependency-free unit tests
examples/            # plugin drop-in + custom provider template
```

Design rule: **`runner.py` must never import Airflow.** Keeping the runner Airflow-free is what makes it unit-testable and reusable.

## Adding a test-data provider

Subclass `DataProvider`, implement `materialize`, and register it:

```python
from dataproc_local.providers import register, DataProvider

@register
class MyProvider(DataProvider):
    name = "myprovider"
    def materialize(self, source: str, dest: str, **opts) -> str:
        # produce data at `dest`, return dest
        return dest
```

Add a test in `tests/` and it'll show up in `dataproc-local providers`.

## Adding support for a new operator

Edit the `mapping` dict in `airflow_patch.py`. Use `_noop_execute(label)` for lifecycle operators and `_submit_execute(runner)` for anything that runs a job. If the operator stores its job spec on an unusual attribute, extend `_submit_execute` to read it.

## Style

- Keep the core dependency-light. Heavy deps (pandas, sqlalchemy, airflow) belong behind optional extras.
- Prefer small, pure functions that are easy to test.
- Every new behavior gets a test.

## Pull requests

1. Branch from `master`.
2. Add/keep tests green (`pytest -q`).
3. Update docs (README/SETUP/QUICKSTART) if behavior or flags change.
4. Open the PR with a clear description of the problem and the fix.

## Reporting bugs

Open an issue with: your Airflow + provider versions, the operator involved, the command you ran, and the log line starting `[dataproc-local]`. A `--dry-run` command output is especially helpful.
