# CI/CD Pipeline

This document describes the automated CI/CD pipeline for **save-gcp-local**.

---

## Overview

```
  Feature branch ──PR──> master ──merge──> Deploy to PyPI + GitHub Release
       │                   │                        │
       └── CI checks ──────┘                        └── Docs rebuild (GitHub Pages)
```

| Trigger | Workflow | What it does |
|---------|----------|-------------|
| Pull request to `master` | **CI** (`ci.yml`) | Lint, test (Python 3.8–3.12), build wheel |
| Push/merge to `master` | **Deploy** (`deploy.yml`) | Test, build, publish to PyPI, create GitHub Release |
| Push to `master` (docs changed) | **Docs** (`docs.yml`) | Rebuild and deploy MkDocs site to GitHub Pages |

---

## Workflows in detail

### 1. CI — runs on every pull request

**File:** `.github/workflows/ci.yml`

**Steps:**

1. **Lint** — checks code formatting and style with `ruff`
2. **Test** — runs `pytest` across Python 3.8, 3.9, 3.10, 3.11, 3.12
3. **Build** — builds the wheel, installs it in a clean venv, and verifies the CLI works

All three must pass before a PR can be merged (enforced by branch protection rules).

### 2. Deploy — runs on merge to master

**File:** `.github/workflows/deploy.yml`

**Steps:**

1. **Test** — runs the test suite on Python 3.8, 3.11, and 3.12 as a safety net
2. **Version check** — reads the version from `pyproject.toml` and checks if a git tag already exists
3. **Build** — builds sdist + wheel, checks with `twine`
4. **Publish** — pushes to PyPI via trusted publishing (no API tokens needed)
5. **Release** — creates a GitHub Release with auto-generated release notes

The deploy is **idempotent**: if the version in `pyproject.toml` hasn't changed since the last release, the publish and release steps are skipped. This means routine documentation or CI-only changes won't trigger a new PyPI release.

### 3. Docs — runs when documentation changes

**File:** `.github/workflows/docs.yml`

Rebuilds the MkDocs site and deploys to GitHub Pages whenever markdown files or `mkdocs.yml` change on `master`.

---

## How to release a new version

1. **Create a feature branch** from `master`
2. **Make your changes** (code, tests, docs)
3. **Bump the version** in `pyproject.toml`
4. **Update `CHANGELOG.md`** with the new version and changes
5. **Open a pull request** to `master`
6. **Wait for CI** — lint, tests, and build must all pass
7. **Get a review and merge** — the merge to `master` triggers the deploy workflow
8. **Done** — PyPI package and GitHub Release are created automatically

```bash
# Example: releasing v0.2.1
git checkout -b release/0.2.1

# Edit pyproject.toml: version = "0.2.1"
# Edit CHANGELOG.md: add ## [0.2.1] section

git add pyproject.toml CHANGELOG.md
git commit -m "Bump version to 0.2.1"
git push -u origin release/0.2.1

# Open PR, wait for CI, merge -> auto-deploys to PyPI
```

---

## Branch protection

Direct pushes to `master` are **not allowed**. All changes must go through a pull request with passing CI checks.

See the [Branch Protection Setup](#branch-protection-setup) section below for how to configure this.

---

## Branch protection setup

This must be configured in GitHub Settings (one-time, by a repo admin):

1. Go to **Settings > Branches** in your GitHub repo
2. Click **Add branch protection rule**
3. Set **Branch name pattern** to `master`
4. Enable:
   - **Require a pull request before merging**
   - **Require status checks to pass before merging**
   - Add these required checks: `lint`, `test`, `build`
   - **Do not allow bypassing the above settings** (optional, even for admins)
5. Click **Create**

---

## PyPI trusted publishing setup

The deploy workflow uses PyPI's [trusted publishing](https://docs.pypi.org/trusted-publishers/) — no API tokens to manage. One-time setup:

1. Go to [pypi.org](https://pypi.org) and log in
2. Navigate to your project **save-gcp-local** > **Settings** > **Publishing**
3. Add a new **GitHub** publisher:
   - **Owner:** `EshwarCVS`
   - **Repository:** `save-gcp-local`
   - **Workflow name:** `deploy.yml`
   - **Environment name:** `pypi`
4. In your GitHub repo, go to **Settings > Environments**
5. Create an environment named `pypi` (if it doesn't exist)

---

## GitHub Pages setup (for documentation site)

1. Go to **Settings > Pages** in your GitHub repo
2. Under **Source**, select **Deploy from a branch**
3. Set branch to `gh-pages` and folder to `/ (root)`
4. Click **Save**
5. The docs site will be live at `https://eshwarcvs.github.io/save-gcp-local`

---

## Local development

### Running tests locally

```bash
pip install -e ".[all,dev]"
pytest -q
```

### Building the docs locally

```bash
pip install mkdocs-material
mkdocs serve
# Open http://127.0.0.1:8000
```

### Building the package locally

```bash
pip install build
python -m build
ls dist/
```
