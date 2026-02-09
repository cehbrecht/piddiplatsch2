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
- Local testing via Docker (Kafka + mock Handle)
- CLI commands: `consume`, `retry`
- Multihash checksum support

---

## 🧪 Testing

Quick checks and end-to-end:

```bash
# Fast tests (unit + integration)
make test

# Unit only / Integration only
make test-unit
make test-integration

# Smoke (starts Docker: Kafka + mock Handle)
make test-smoke
```

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

## 🚀 Usage

> ⚠️ **Requires external Kafka and Handle services to be running.**  
> 💡 For local testing/development, see [CONTRIBUTING.md](CONTRIBUTING.md) for Docker setup.

### Start the consumer

```bash
piddi consume
```

### Options

- **Verbose**: `--verbose`
- **Debug/log file**: `--debug --log my.log`
- **Dump messages**: `--dump` (writes JSONL files under `outputs/dump/`)
- **Dry-run**: `--dry-run` (writes handle records to JSONL without contacting Handle Service)
- **Force-continue**: `--force` (continue on transient external failures like STAC outages; still persists skipped messages)

```bash
piddi --config custom.toml --verbose --debug --log my.log consume --dump
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

 

## 🧩 Plugins (Concise)

- One plugin active at a time, selected via `consumer.processor` (e.g., `"cmip6"`).
- Static registry: CMIP6 is registered by default; future plugins (e.g., `cordex`) are added explicitly.
- Config lives under `[plugins.<name>]`, for example:

```toml
[plugins.cmip6]
landing_page_url = "https://handle-esgf.dkrz.de/lp"
max_parts = -1
excluded_asset_keys = ["reference_file", "globus", "thumbnail", "quicklook"]
```

To add a plugin:
- Implement a processor (subclass of `BaseProcessor`) under `src/piddiplatsch/plugins/<name>/processor.py`.
- Register it in the static registry (see `piddiplatsch.core.registry.register_processor("<name>", YourProcessor)`).
- Provide `[plugins.<name>]` config as needed.

Note: Design is intentionally simple and may evolve; no dynamic framework is required.

