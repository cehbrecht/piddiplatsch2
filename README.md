# Piddiplatsch

[![Build Status](https://github.com/cehbrecht/piddiplatsch2/actions/workflows/ci.yml/badge.svg)](https://github.com/cehbrecht/piddiplatsch2/actions)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Python Version](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![pre-commit enabled](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://pre-commit.com/)


---

**Piddiplatsch** is a [Kafka](https://kafka.apache.org/) consumer for CMIP6+ records that integrates with a [Handle Service](https://pypi.org/project/pyhandle/) to reliably register persistent identifiers (PIDs).  
*Curious by nature. Persistent by design.*

Inspired by the TV puppet [Pittiplatsch](https://en.wikipedia.org/wiki/Pittiplatsch), the name reflects more than wordplay. “Pitti” gives us the CLI name `piddi`, while the PID pun is purely phonetic. Like its namesake, Piddiplatsch is curious, persistent, and unafraid of a little chaos: it jumps into streaming data, handles errors head-on, and keeps going until the job is done.

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
piddi --help  # commands: consume and retry
piddi consume --help
piddi --verbose consume 
```

Optional: customize config by copying [src/piddiplatsch/config/default.toml](src/piddiplatsch/config/default.toml) to `custom.toml` and run with `--config custom.toml`.

Dry-run (no Handle Service calls):

```bash
piddi --config custom.toml --verbose consume --dry-run
# optionally also dump messages
piddi --config custom.toml --verbose consume --dry-run --dump
```

---

## ✨ Features

- Kafka consumer for CMIP6+ records
- Register/update PIDs via Handle Service
- CLI commands: `consume`, `retry`
- Multihash checksum support
- Simple plugin support: select via `consumer.processor` (e.g., "cmip6")

For full usage details and local Docker smoke tests, see [CONTRIBUTING.md](CONTRIBUTING.md).

---

## 🚀 Usage (Overview)

Basics:
- Start consumer: `piddi consume`
- Pass a config: `piddi --config custom.toml`
- Use `--dry-run` and `--dump` for safe local inspection

Detailed options and the observe-mode example live in [CONTRIBUTING.md](CONTRIBUTING.md).

---

## 🛠️ Configuration

You can customize Kafka or Handle settings:

```bash
cp src/piddiplatsch/config/default.toml custom.toml
vim custom.toml
```

Use the `config` option to use your custom configuration:

```bash
piddi --config custom.toml
```

---

## 🧪 Testing

Quick examples:
- All tests (unit + integration): `make test`
- Unit only: `make test-unit`
- Integration only: `make test-integration`
- Smoke (Docker): `make test-smoke`

Full guidance (env, Docker details) is in [CONTRIBUTING.md](CONTRIBUTING.md).

---

## 🔄 Recovery & Retry (Concise)

- Failures: `outputs/failures/r<N>/failed_items_<date>.jsonl`
- Skipped (transient): `outputs/skipped/skipped_items_<date>.jsonl` (use `--force` to continue)
- Dumps: `outputs/dump/dump_messages_<date>.jsonl` (use `--dump`)

Retry previously persisted items:

```bash
piddi retry <path...> [--delete-after] [--dry-run] [-v]
```

See implementation details in [src/piddiplatsch/persist/retry.py](src/piddiplatsch/persist/retry.py) and recorders under `src/piddiplatsch/persist/`.

---

 

## 🧩 Plugins (Overview)

Select a plugin via `consumer.processor` (e.g., "cmip6"). Configuration and implementation guidance are documented in [CONTRIBUTING.md](CONTRIBUTING.md).

---

## 🤝 Contributing

Interested in contributing? See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, testing, style, and workflow.

