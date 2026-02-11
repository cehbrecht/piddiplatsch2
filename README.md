# Piddiplatsch

[![Build Status](https://github.com/cehbrecht/piddiplatsch2/actions/workflows/ci.yml/badge.svg)](https://github.com/cehbrecht/piddiplatsch2/actions)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Python Version](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![pre-commit enabled](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://pre-commit.com/)

---

**Piddiplatsch** is a [Kafka](https://kafka.apache.org/) consumer for **CMIP6 records** that integrates with a [Handle Service](https://pypi.org/project/pyhandle/) to reliably register and maintain persistent identifiers (PIDs).

*Curious by nature. Persistent by design.*

Inspired by the TV puppet [Pittiplatsch](https://en.wikipedia.org/wiki/Pittiplatsch), the name reflects more than wordplay.  
“Pitti” gives us the CLI name `piddi`, while the PID pun is purely phonetic. Like its namesake, Piddiplatsch is curious, persistent, and unafraid of a little chaos: it jumps into streaming data, handles errors head-on, and keeps going until the job is done.

---

## 🎯 Intended Audience

Piddiplatsch is primarily developed and used at **DKRZ** in the context of CMIP data ingestion and PID registration workflows.

It is intended for users who:
- work with CMIP6-style dataset or file records
- are comfortable running Kafka consumers
- need to register or update PIDs via a Handle.Net service

The project is fully open-source and documented. Curious users or future adopters with similar requirements are welcome to explore and reuse it.

---

## 🧭 About CMIP6 (and the future)

At the moment, **Piddiplatsch processes CMIP6-style records only**.  
CMIP7 does not yet exist as a concrete standard.

The codebase is intentionally structured so that future CMIP phases (e.g. CMIP7) can be supported by adding a new processor, without rewriting the consumer core. The existing `cmip6` processor serves both as:

- the production implementation today
- a reference implementation for future extensions

---

## ⚡ Quick Start

Install, run, and test in minutes (CLI: `piddi`):

```bash
# 1) Setup environment
git clone git@github.com:cehbrecht/piddiplatsch2.git
cd piddiplatsch2
conda env create && conda activate piddi
make develop

# 2) Run tests
make test            # unit + integration

# 3) Run the consumer (requires Kafka + Handle)
piddi --help         # commands: consume, retry
piddi consume --help
piddi --verbose consume
```

**Prerequisites for real runs**

You need a Kafka broker and a Handle Service (or mock Handle server) available.  
For safe local exploration, you can use `--dry-run` or observe mode (see below).

---

## 🧪 Safe Exploration (Dry-Run & Observe)

Dry-run mode disables all Handle Service writes:

```bash
piddi --config custom.toml --verbose consume --dry-run
# optionally also dump messages
piddi --config custom.toml --verbose consume --dry-run --dump
```

### Observe Mode (Example)

For exploratory runs without external dependencies, use the relaxed example config:

```bash
# Option A: reference the file in-place
piddi --config etc/observe.toml consume --dry-run --dump --force

# Option B: copy and run locally
cp etc/observe.toml .
piddi --config observe.toml consume --dry-run --dump --force
```

What this does:
- no external Handle Service calls
- records written locally as JSONL
- continues through transient skips (`--force`)
- dumps incoming messages to `outputs/dump/` when `--dump` is used

See the configuration at [etc/observe.toml](etc/observe.toml).

---

## ✨ Features

- Kafka consumer for CMIP6 records
- Register and update PIDs via Handle Service
- CLI commands: `consume`, `retry`
- Multihash checksum support
- Simple processor mechanism (pure Python, no plugin framework)
- Designed for future CMIP phases via additional processors

For full usage details and local Docker smoke tests, see [CONTRIBUTING.md](CONTRIBUTING.md).

---

## 🚀 Usage (Overview)

Common first runs:

- Inspect messages only:
  ```bash
  piddi consume --dry-run --dump
  ```
- Observe without stopping on skips:
  ```bash
  piddi consume --dry-run --force
  ```
- Use a custom configuration:
  ```bash
  piddi --config custom.toml consume
  ```

Detailed CLI options and extended examples live in [CONTRIBUTING.md](CONTRIBUTING.md).

---

## 🛠️ Configuration

Start from the default configuration:

```bash
cp src/piddiplatsch/config/default.toml custom.toml
vim custom.toml
```

Run with your custom configuration:

```bash
piddi --config custom.toml
```

Kafka, Handle Service, consumer behaviour, and processor selection are all controlled via this file.

### Validate Config

```bash
piddi --config custom.toml config validate
```

Exits non-zero on errors; prints warnings when applicable.

### Show Effective Config

```bash
piddi --config custom.toml config show           # TOML
piddi --config custom.toml config show --format json
piddi --config custom.toml config show --section consumer
piddi --config custom.toml config show --section kafka --key group.id
```

Prints the merged defaults + your overrides for quick inspection.

---

## 🔄 Recovery & Retry

Piddiplatsch persists problematic records for later inspection or retry.

Failure records are written to:

```
outputs/failures/r<N>/failed_items_<date>.jsonl
```

Skipped (transient) records are written to:

```
outputs/skipped/skipped_items_<date>.jsonl
```

Dumped messages are written to:

```
outputs/dump/dump_messages_<date>.jsonl
```

Retry previously persisted items:

```bash
piddi retry <path...> [--delete-after] [--dry-run] [-v]
```

Implementation details:
- Retry logic: [src/piddiplatsch/persist/retry.py](src/piddiplatsch/persist/retry.py)
- Recorders: `src/piddiplatsch/persist/`

---

## 🧩 Processors (Overview)

Piddiplatsch uses a small, explicit **processor interface** to handle record formats.

This is **not** a dynamic plugin ecosystem. The mechanism exists to:
- isolate CMIP6-specific logic
- allow future formats (e.g. CMIP7) to be added cleanly
- keep testing and evolution predictable

Currently, the only supported processor is:

- `cmip6` (default)

Configuration and implementation guidance are documented in [CONTRIBUTING.md](CONTRIBUTING.md).

---

## 🧪 Testing

Quick commands:
- All tests (unit + integration): `make test`
- Unit only: `make test-unit`
- Integration only: `make test-integration`
- Smoke tests (Docker): `make test-smoke`

Full development and testing guidance is in [CONTRIBUTING.md](CONTRIBUTING.md).

---

## 🤝 Contributing

Interested in contributing?  
See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, testing, style, and workflow.

