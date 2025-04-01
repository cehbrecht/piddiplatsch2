# Piddiplatsch

## Overview
Piddiplatsch is a Kafka consumer for CMIP7 records that integrates with a Handle Service for persistent identifiers (PIDs).

### Kafka

https://kafka.apache.org/


## Features
- Listens to a Kafka topic for CMIP7 records
- Adds, updates, and deletes PIDs in a Handle Service
- Caches PID search results
- Includes a mock Handle Server for testing

## Installation

Create the conda environment:
```sh
conda env create
conda activate piddiplatsch2
```

Install required packages with:
```sh
pip install -e ".[dev]"
```

## Setup
1. Start Kafka with:
   ```sh
   docker-compose up -d
   ```

2. Run the mock Handle Server:
   ```sh
   python src/piddiplatsch/mock_handle_server.py
   ```

3. Run the Kafka consumer:
   ```sh
   python src/piddiplatsch/consumer.py
   ```


