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
piddiplatsch --help  # two commands: consume and retry
piddiplatsch consume --help
piddiplatsch --verbose consume 
```

Optional: customize config by copying [src/piddiplatsch/config/default.toml](src/piddiplatsch/config/default.toml) to `custom.toml` and run with `--config custom.toml`.

Dry-run (no Handle Service calls):

```bash
piddiplatsch --config custom.toml --verbose consume --dry-run
# optionally also dump messages
piddiplatsch --config custom.toml --verbose consume --dry-run --dump
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

> ⚠️ **Requires external Kafka and Handle services to be running.**  
> 💡 For local testing/development, see [CONTRIBUTING.md](CONTRIBUTING.md) for Docker setup.

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

## 🤝 Contributing

Interested in contributing? Check out our [CONTRIBUTING.md](CONTRIBUTING.md) for:
- Development setup
- Testing guidelines (unit, integration, smoke tests)
- Code style and linting
- Version management
- Development workflow

---

## 🔄 Failure Recovery

Failed items are saved to `outputs/failures/r<N>/failed_items_<date>.jsonl` for later retry.

### Retry Command

Reprocess failed items directly through the pipeline (no Kafka write required):

```bash
piddiplatsch retry <failure-file.jsonl> [--delete-after]
```

**Examples:**

```bash
# Retry failed items
piddiplatsch retry outputs/failures/r0/failed_items_2026-01-16.jsonl

# Retry in dry-run mode (test without contacting Handle Service)
piddiplatsch retry outputs/failures/r0/failed_items_2026-01-16.jsonl --dry-run

# Retry and delete file on success
piddiplatsch retry outputs/failures/r0/failed_items_2026-01-16.jsonl --delete-after
```

Items are reprocessed with incremented retry counters. New failures go to `r1/`, `r2/`, etc.

---

##  Examples

Start consumer with custom configuration and dump messages:

```bash
piddiplatsch --config custom.toml --verbose consume --dump
```

---

## ✅ TODO

- [ ] **Batch Handle registration**  
  Support committing one dataset and its associated files in a single batch request.

- [ ] **Plugin improvements**  
  Enhance plugin system to better support multiple processing use-cases.

