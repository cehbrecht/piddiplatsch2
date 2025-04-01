# Piddiplatsch

## Overview
Piddiplatsch is a Kafka consumer for CMIP7 records that integrates with a Handle Service for persistent identifiers (PIDs).

## Features
- Listens to a Kafka topic for CMIP7 records
- Adds, updates, and deletes PIDs in a Handle Service
- Caches PID search results
- Includes a mock Handle Server for testing

## Setup
1. Start Kafka with:
   ```sh
   docker-compose up -d
   ```

2. Run the mock Handle Server:
   ```sh
   python piddiplatsch/mock_handle_server.py
   ```

3. Run the Kafka consumer:
   ```sh
   python piddiplatsch/consumer.py
   ```

## Dependencies
Install required packages with:
```sh
pip install -r requirements.txt
```
