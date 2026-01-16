# Piddiplatsch

[![Build Status](https://github.com/cehbrecht/piddiplatsch2/actions/workflows/ci.yml/badge.svg)](https://github.com/cehbrecht/piddiplatsch2/actions)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Python Version](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![pre-commit enabled](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://pre-commit.com/)


---

**Piddiplatsch** is a [Kafka](https://kafka.apache.org/) consumer for CMIP6+ records that integrates with a [Handle Service](https://pypi.org/project/pyhandle/) for persistent identifiers (PIDs).

---

## ⚡ Quick Start

Install, run, and test in minutes:

```bash
# 1) Setup environment
git clone git@github.com:cehbrecht/piddiplatsch2.git
cd piddiplatsch2
conda env create && conda activate piddiplatsch2
make develop

# 2) Run tests
make test            # unit + integration

# 3) Run the consumer (requires Kafka + Handle)
piddiplatsch consume --help
piddiplatsch consume --verbose
```

Optional: customize config by copying [src/piddiplatsch/config/default.toml](src/piddiplatsch/config/default.toml) to `custom.toml` and run with `--config custom.toml`.

Dry-run (no Handle Service calls):

```bash
piddiplatsch --config custom.toml consume --verbose --dry-run
# optionally also dump messages
piddiplatsch --config custom.toml consume --verbose --dry-run --dump
```

---

## ✨ Features

- Kafka consumer for CMIP6+ records
- Register/update PIDs via Handle Service
- Local testing via Docker (Kafka + mock Handle)
- CLI commands: `consume`, `retry`
- Multihash checksum support

---

## ⚙️ Installation

**Prerequisites**:
- [Miniconda (via conda-forge)](https://conda-forge.org/download/)

**Setup**:

```bash
git clone git@github.com:cehbrecht/piddiplatsch2.git
cd piddiplatsch2

conda env create
conda activate piddiplatsch2

# Install dependencies
pip install -e ".[dev]"
# OR
make develop
```

---

## 🛠️ Configuration

You can customize Kafka or Handle settings:

```bash
cp src/piddiplatsch/config/default.toml custom.toml
vim custom.toml
```

Use the `config` option to use your custom configuration:

```bash
piddiplatsch --config custom.toml
```

---

## 🚀 Usage

> ⚠️ **Kafka and Handle service must be running!**  
> 💡 Use Docker setup below for local testing.

### Start the consumer

```bash
piddiplatsch consume
```

### Options

- **Verbose**: `--verbose`
- **Debug/log file**: `--debug --log my.log`
- **Dump messages**: `--dump` (writes JSONL files under `outputs/dump/`)
- **Dry-run**: `--dry-run` (writes handle records to JSONL without contacting Handle Service)

```bash
piddiplatsch --config custom.toml --verbose --debug --log my.log consume --dump
```

---

## ✅ Running Tests

This project uses a three-tier testing strategy:

### Test Types

**Unit Tests** (fast, no external dependencies)
- Located in `tests/`
- Pure logic tests with mocked dependencies
- No markers required

**Integration Tests** (medium speed, JSONL backend)
- Located in `tests/integration/`
- Tests component interactions using JSONL backend
- Marked with `@pytest.mark.integration`
- No Docker required

**Smoke Tests** (end-to-end, requires Docker)
- Located in `tests/smoke/`
- Full workflow tests with Kafka + Handle server
- Marked with `@pytest.mark.smoke`
- Docker services started/stopped automatically
- **Note:** Docker is only used for testing, not required for production
 - Shared test configuration: tests use [tests/config.toml](tests/config.toml)

### Running Tests

Run all fast tests (unit + integration):

```bash
make test
```

Run only unit tests:

```bash
make test-unit
```

Run only integration tests:

```bash
make test-integration
```

Run smoke tests (Docker services started automatically):

```bash
make smoke
# or
make test-smoke
```

Run all tests including smoke tests:

```bash
make test-all
```

### Docker Management (for testing)

Start Docker services manually (Kafka + Handle server):

```bash
make start-docker
```

Stop Docker services:

```bash
make stop-docker
```

Rebuild Docker images:

```bash
make docker-build
```

Clean up Docker images and volumes:

```bash
make docker-clean
```

---

## 🧼 Code Style and Linting

This project uses [pre-commit](https://pre-commit.com) to enforce code style and quality:

- [`black`](https://black.readthedocs.io) for code formatting  
- [`isort`](https://pycqa.github.io/isort/) for import sorting  
- [`ruff`](https://docs.astral.sh/ruff/) for linting and fast checks

### Setup

```bash
pip install pre-commit
pre-commit install
```

### Run manually

```bash
pre-commit run --all-files
```

Or use:

```bash
make lint        # Run ruff, black, and isort checks
make format      # Auto-format with black and isort
make check-format  # Check formatting only
```

---

## Failure Recovery and Retry

When `piddiplatsch` fails to register or process a STAC item from Kafka, the failed item is saved for recovery in a JSON Lines (`.jsonl`) format. This enables you to preserve thousands of failure records for later inspection and retry.

### Where failures are stored

- Saved under `outputs/failures/retries-<n>/failed_items_<date>.jsonl` in the configured `output_dir`.

### Retrying Failed Items

Use the `retry` CLI command to resend failed items from a `.jsonl` file back into Kafka for reprocessing.

```bash
piddiplatsch retry <failure-file.jsonl> [--delete-after]
```

Options:
- `<failure-file.jsonl>`: failure file to retry
- `--delete-after`: delete file after successful retry

### Example

```bash
piddiplatsch retry outputs/failures/retries-0/failed_items_2025-07-23.jsonl --delete-after
```

Resends items to the configured Kafka retry topic, incrementing retry count. With `--delete-after`, removes the file on success.

---

## 📦 Versioning

Uses bump-my-version to update the project version and create a git tag.

```bash
# dry-run (no commit/tag)
bump-my-version bump patch --dry-run --allow-dirty

# bump and tag
make bump-patch    # or: make bump-minor / make bump-major
git push && git push --tags
```

---

## 📓 Examples

Start consumer with custom configuration and dump messages:

```bash
piddiplatsch --config custom.toml consume --verbose --dump
```

---

## ✅ TODO

- [ ] **Batch Handle registration**  
  Support committing one dataset and its associated files in a single batch request.

- [ ] **Plugin improvements**  
  Enhance plugin system to better support multiple processing use-cases.

