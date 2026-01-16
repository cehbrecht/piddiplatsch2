# Piddiplatsch

[![Build Status](https://github.com/cehbrecht/piddiplatsch2/actions/workflows/ci.yml/badge.svg)](https://github.com/cehbrecht/piddiplatsch2/actions)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Python Version](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![pre-commit enabled](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://pre-commit.com/)


---

**Piddiplatsch** is a [Kafka](https://kafka.apache.org/) consumer for CMIP6+ records that integrates with a [Handle Service](https://pypi.org/project/pyhandle/) for persistent identifiers (PIDs).

---

## ✨ Features

- Listens to a Kafka topic for CMIP6+ records
- Adds and updates PIDs via a Handle Service
- Includes a mock Handle Server for local testing
- Includes a Kafka service with docker-compose for testing
- CLI support to start kafka consumer
- Supports multihash checksums

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
cp src/config/default.toml custom.toml
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

### Start the Kafka consumer:

```bash
piddiplatsch consume
```

### With verbose:

In verbose mode you will get a progress bar on the console.

```bash
piddiplatsch --verbose consume
```

In verbose mode the consumer shows a performance message on the console.

### Change logging:

```bash
piddiplatsch --debug --log my.log consume
```

You can enable debug logging and also change the default log file (`pid.log`).

### Optionally dump all messages:

```bash
piddiplatsch consume --dump
```

The messages are written to `outputs/dump/` as json-lines files. For example:

```bash
outputs/dump/dump_messages_2025-11-03.jsonl
```

### Dry-run (no handle server)

Write handle records to disk only without contacting the Handle Service.

```bash
piddiplatsch consume --dry-run
```

- Uses a JSONL backend and writes to `outputs/handles/handles_<date>.jsonl`.
- Temporarily overrides any configured handle backend for this run.
- Can be combined with message dumping:

```bash
piddiplatsch consume --dry-run --dump
```

### Example

Run consumer with custom configuration in verbose mode and dump all messages:

```bash
piddiplatsch --config custom.toml --verbose consume --dump
```

Run consumer in dry-run mode (no Handle Service calls), still dumping messages:

```bash
piddiplatsch --config custom.toml --verbose consume --dry-run --dump
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

## 🐳 Docker for Testing

> 💡 **Note:** Docker services are only used for smoke tests. Production deployments use external Kafka and Handle services.

Start Kafka and mock Handle service for testing:

```bash
make start-docker
```

Stop all services:

```bash
make stop-docker
```

Smoke tests exercise Kafka (Docker services started automatically). To run them:

```bash
make smoke
```

This command also starts the production consumer in the background and stops it (and Docker services) after the tests finish.

 

---

## Failure Recovery and Retry

When `piddiplatsch` fails to register or process a STAC item from Kafka, the failed item is saved for recovery in a JSON Lines (`.jsonl`) format. This enables you to preserve thousands of failure records for later inspection and retry.

### How Failures Are Stored

* Failed items are saved under the configured `output_dir` (default: `outputs/failures`).
* Failures are grouped by UTC date in files named like `failed_items_YYYY-MM-DD.jsonl`.
* To track retry attempts, failures are stored in subfolders named by retry count:

```
outputs/
└── failures/
    ├── retries-0/          # First failures (no retries yet)
    │   └── failed_items_2025-07-23.jsonl
    ├── retries-1/          # First retry attempt
    │   └── failed_items_2025-07-23.jsonl
    └── retries-2/          # Second retry attempt
        └── failed_items_2025-07-23.jsonl
```

* Each JSON object includes a `"failure_timestamp"` (UTC ISO8601) and `"retries"` count.

### Retrying Failed Items

Use the `retry` CLI command to resend failed items from a `.jsonl` file back into Kafka for reprocessing.

```bash
piddiplatsch retry <failure-file.jsonl> [--delete-after]
```

Options:

* `<failure-file.jsonl>`: Path to the failure file to retry.
* `--delete-after`: Delete the file after all messages have been retried successfully.

### Example

```bash
piddiplatsch retry outputs/failures/retries-0/failed_items_2025-07-23.jsonl --delete-after
```

This command will resend all items from that failure file to the configured Kafka retry topic, increasing their retry count automatically. If all messages succeed, the file will be deleted.

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

Run consumer with custom configuration in verbose mode and dump all messages:

```bash
piddiplatsch --config custom.toml --verbose consume --dump
```

Run consumer in dry-run mode (no Handle Service calls), still dumping messages:

```bash
piddiplatsch --config custom.toml --verbose consume --dry-run --dump
```

---

## ✅ TODO

- [ ] **Batch Handle registration**  
  Support committing one dataset and its associated files in a single batch request.

- [ ] **Plugin improvements**  
  Enhance plugin system to better support multiple processing use-cases.

