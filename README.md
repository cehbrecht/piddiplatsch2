# Piddiplatsch

## Overview
Piddiplatsch is a Kafka consumer for CMIP7 records that integrates with a Handle Service for persistent identifiers (PIDs).

### Kafka

Docs:

* https://kafka.apache.org/
* https://pypi.org/project/kafka-python/

Examples:
* https://github.com/katyagorshkova/kafka-kraft

### Handle Client

https://pypi.org/project/pyhandle/


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

## Run kafka

Start Kafka with:
```sh
docker-compose up --build -d
```

Stop Kafka with:
```sh
docker-compose down -v
```

## Use kafka client

Run the `kafka.sh` script :

```sh
# create topic cmip7
./scripts/kafka.sh create cmip7

# list all topics
./scripts/kafka.sh list

# send message to topic cmip7
./scripts/kafka.sh send cmip7 hi

# consume all messages from topic cmip7
./scripts/kafka.sh consume cmip7  
```

## Use piddiplatsch client

Create topic CMIP7:
```sh
piddiplatsch init
```

Send a message:
```sh
piddiplatsch send -m '{"greetings": "hey"}'
```

Consume messages:
```sh
piddiplatsch consume
```

## Example with PIDs

Add a PID record:
```sh
piddiplatsch send -m '{"action": "add", "record": {"pid": "1234", "name": "tas-2025-04-16.nc"}}'
```



