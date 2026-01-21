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

Reprocess failed items directly through the pipeline (no Kafka write required):

```bash
piddiplatsch retry <path...> [--delete-after] [--dry-run]
```

Supports:
- Individual files: `retry file1.jsonl file2.jsonl`
- Directories: `retry outputs/failures/r0/`
- Multiple paths: `retry outputs/failures/r0/ outputs/failures/r1/`
- Skipped files: `retry outputs/skipped/` (reprocess skipped items captured during `--force` runs)

**Examples:**

```bash
# Retry a single file
piddiplatsch retry outputs/failures/r0/failed_items_2026-01-16.jsonl

# Retry all files in a directory
piddiplatsch retry outputs/failures/r0/
# Retry skipped messages
piddiplatsch retry outputs/skipped/

# Retry multiple files
piddiplatsch retry file1.jsonl file2.jsonl file3.jsonl

# Retry in dry-run mode (test without contacting Handle Service)
piddiplatsch retry outputs/failures/r0/ --dry-run

# Retry with progress indicators (verbose mode)
piddiplatsch --verbose retry outputs/failures/r0/

# Retry and delete files on success
piddiplatsch retry outputs/failures/r0/ --delete-after
```

**Progress Indicators:**

Use `--verbose` flag to see per-file progress during retry:

```bash
piddiplatsch --verbose retry outputs/failures/r0/

# Output:
# [1/3] failed_items_2026-01-15.jsonl: 45/50 succeeded, 5 failed
# [2/3] failed_items_2026-01-16.jsonl: 100/100 succeeded
# [3/3] failed_items_2026-01-17.jsonl: 23/25 succeeded, 2 failed
#
# Total: 168/175 succeeded
#   ⚠️  7 items failed again (96.0% success rate)
#   New failures saved to:
#     - r1/failed_items_2026-01-16.jsonl
```

**Detailed Feedback:**

The retry command provides comprehensive feedback:
- Per-file success/failure counts
- Overall statistics and success rate
- Location of new failure files (if any items fail again)

Example output:
```
Found 3 file(s) to retry.
  failed_items_2026-01-15.jsonl: 45/50 succeeded, 5 failed
  failed_items_2026-01-16.jsonl: 100/100 succeeded
  failed_items_2026-01-17.jsonl: 23/25 succeeded, 2 failed

Total: 168/175 succeeded
  ⚠️  7 items failed again (96.0% success rate)
  New failures saved to:
    - r1/failed_items_2026-01-16.jsonl
```

Items are reprocessed with incremented retry counters. New failures go to `r1/`, `r2/`, etc.

**Manual Testing:**

A sample failure JSONL file is available for testing:

```bash
# Test retry with sample data (dry-run mode)
piddiplatsch retry tests/testdata/sample_failures.jsonl --dry-run
```

The sample file [`tests/testdata/sample_failures.jsonl`](tests/testdata/sample_failures.jsonl) contains 3 failed CMIP6 STAC items with realistic metadata. Use this to:
- Test the retry command without setting up Kafka
- Verify your processor handles different failure scenarios
- Practice working with failure recovery workflows

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

