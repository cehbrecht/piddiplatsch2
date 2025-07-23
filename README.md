# Piddiplatsch

[![Build Status](https://github.com/cehbrecht/piddiplatsch2/actions/workflows/ci.yml/badge.svg)](https://github.com/cehbrecht/piddiplatsch2/actions)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Python Version](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![View Notebooks on nbviewer](https://img.shields.io/badge/nbviewer-view%20notebooks-orange)](https://nbviewer.org/github/cehbrecht/piddiplatsch2/tree/main/notebooks/)

---

**Piddiplatsch** is a [Kafka](https://kafka.apache.org/) consumer for CMIP6+ records that integrates with a [Handle Service](https://pypi.org/project/pyhandle/) for persistent identifiers (PIDs).

---

## ✨ Features

- Listens to a Kafka topic for CMIP6+ records
- Adds, updates, and deletes PIDs via a Handle Service
- Includes a mock Handle Server for local testing
- Includes a Kafka service with docker-compose for testing
- CLI and plugin support
- Example notebooks

---

## ⚙️ Installation

**Prerequisites**:
- [Miniconda or Mamba (via conda-forge)](https://conda-forge.org/download/)

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
cp src/config/default_config.toml custom.toml
vim custom.toml
```

Use your config file when running:

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

### With debug and log:

```bash
piddiplatsch --debug --logfile consume.log consume
```

---

## ✅ Running Tests

Run all unit tests:

```bash
make test
```

Run smoke tests (Kafka and Handle service must be up):

```bash
make smoke
```

---

## 🐳 Local Kafka with Docker

Start Kafka and mock Handle service:

```bash
docker-compose up --build -d
# OR
make start
```

Stop all services:

```bash
docker-compose down -v
# OR
make stop
```

### Initialize the Kafka topic for testing only:

```bash
piddiplatsch init
```

### Send a record (JSON format) for testing only:

```bash
piddiplatsch send -p tests/testdata/CMIP6/<your_file>.json
```

---

Got it! Here’s the failure recovery section ready for your README:

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

## Failure Recovery and Retry

Failed STAC items are saved as JSON Lines (`.jsonl`) in `outputs/failures`, grouped by date and retry count in folders like `r0`, `r1`, etc.

Each record includes a timestamp and retry count.

To retry failed items, run:

```bash
piddiplatsch retry <file.jsonl> [--delete-after]
```

The --delete-after option removes the file if all retries succeed.

Example:
```bash
piddiplatsch retry outputs/failures/r0/failed_items_2025-07-23.jsonl --delete-after
```

---

## 📓 Examples

Explore the example notebooks here:  
🔗 [nbviewer.org/github/cehbrecht/piddiplatsch2/tree/main/notebooks/](https://nbviewer.org/github/cehbrecht/piddiplatsch2/tree/main/notebooks/)

---

## ✅ TODO

- [ ] **Batch Handle registration**  
  Support committing one dataset and its associated files in a single batch request.

- [ ] **version lookup example**  
  Add an example demonstrating how to retrieve dataset versions via the STAC catalog.

- [ ] **Plugin improvements**  
  Enhance plugin system to better support multiple processing use-cases.

- [ ] **Performance tests with locust.io**  
  Add [locust.io](https://locust.io/) tests to check performance.
