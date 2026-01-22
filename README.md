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
- **Force-continue**: `--force` (continue on transient external failures like STAC outages; still persists skipped messages)

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

### Persistence API (Short)

Recorders share a unified instance API via `RecorderBase`:

```python
from piddiplatsch.persist.dump import DumpRecorder
from piddiplatsch.persist.skipped import SkipRecorder
from piddiplatsch.persist.recovery import FailureRecorder

DumpRecorder().record(key, data)
SkipRecorder().record(key, data, reason="timeout", retries=1)
FailureRecorder().record(key, data, reason="error", retries=2)
```

Helpers for JSONL I/O and daily rotation live in `piddiplatsch.persist.helpers` (`DailyJsonlWriter`, `read_jsonl`, `find_jsonl`).

### Dumped Messages

When running the consumer with `--dump`, all consumed messages are appended to
`outputs/dump/dump_messages_<date>.jsonl` (one JSON object per line). This is useful for
debugging, audits, or crafting reproducible tests. Dump files contain raw messages and
do not include the failure metadata (`__infos__`) used by the retry workflow.

Example run with dump enabled:

```bash
piddiplatsch --config custom.toml --verbose consume --dump
```

### Skipped Messages (Transient External Failures)

Transient external failures (e.g., STAC unreachable, timeouts, HTTP 5xx) are recorded under `outputs/skipped/skipped_items_<date>.jsonl` when running with `--force`. By default (production), the consumer stops on such failures after bounded retries.

```bash
# Continue despite transient failures (records skipped)
piddiplatsch consume --force
```

Policy:
- Permanent-invalid (e.g., missing payload/item, invalid JSON Patch) → treated as errors and counted toward `max_errors`.
- Transient-external (e.g., STAC down/timeouts/5xx) → stop the consumer unless `--force`.

Config knobs (in your TOML):
- `consumer.stop_on_transient_skip = true` (default)
- `consumer.transient_retries = 3`
- `consumer.transient_backoff_initial = 0.5`
- `consumer.transient_backoff_max = 5.0`
- `consumer.preflight_stac = true` (probe STAC health at startup)

### Retry Command

Retry previously persisted items (failures or skipped) through the pipeline:

```bash
piddiplatsch retry <path...> [--delete-after] [--dry-run] [-v]
```

- Accepts files, directories, or glob patterns
- Increments retry counters and reprocesses items
- Saves new failures under `outputs/failures/r<N>/`
 - Also supports retriable skipped files under `outputs/skipped/` (from `--force` runs)

Examples:

```bash
# Single file
piddiplatsch retry outputs/failures/r0/failed_items_2026-01-16.jsonl

# Directory (all JSONL files)
piddiplatsch retry outputs/failures/r0/

# Dry-run (no Handle Service)
piddiplatsch retry outputs/failures/r0/ --dry-run
```

Programmatic usage via `RetryRunner`:

```python
from pathlib import Path
from piddiplatsch.persist.retry import RetryRunner

# Configure once, reuse across files
runner = RetryRunner(
  "cmip6",                      # processor/plugin name
  failure_dir=Path("outputs/failures"),
  delete_after=False,            # delete source file if all succeed
  dry_run=True,                  # do not contact Handle Service
)

# Single file
result = runner.run_file(Path("outputs/failures/r0/failed_items_2026-01-16.jsonl"))
print(result.succeeded, result.failed)

# Batch
overall = runner.run_batch((
  Path("outputs/failures/r0"),
  Path("outputs/failures/r1/failed_items_2026-01-17.jsonl"),
))
print(overall.total, overall.success_rate)
```

For more details, see [src/piddiplatsch/persist/retry.py](src/piddiplatsch/persist/retry.py).

### Failure Recording

Persist failures during consumption for later retry:

```python
from piddiplatsch.persist.recovery import FailureRecorder

FailureRecorder().record(key, data, reason="error", retries=1)
```

See [src/piddiplatsch/persist/recovery.py](src/piddiplatsch/persist/recovery.py) for details.

---

##  Examples

Start consumer with custom configuration and dump messages:

```bash
piddiplatsch --config custom.toml --verbose consume --dump
```

Tip: Use `--force` to continue despite transient external failures (records skipped items under `outputs/skipped/`).

---

## ✅ TODO

- [ ] **Batch Handle registration**  
  Support committing one dataset and its associated files in a single batch request.

- [ ] **Plugin improvements**  
  Enhance plugin system to better support multiple processing use-cases.

