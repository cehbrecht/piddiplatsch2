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
cp src/config/default_config.toml my-config.toml
vim my-config.toml
```

Use your config file when running:

```bash
piddiplatsch --config my-config.toml
```

---

## 🚀 Usage

> ⚠️ **Kafka and Handle service must be running!**  
> 💡 Use Docker setup below for local testing.

### Initialize the Kafka topic:

```bash
piddiplatsch init
```

### Send a record (STAC JSON format):

```bash
piddiplatsch send -p tests/testdata/CMIP6/<your_file>.json
```

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

---

## 📓 Examples

Explore the example notebooks here:  
🔗 [nbviewer.org/github/cehbrecht/piddiplatsch2/tree/main/notebooks/](https://nbviewer.org/github/cehbrecht/piddiplatsch2/tree/main/notebooks/)

---

## 📄 License

Licensed under the [Apache License 2.0](LICENSE).
