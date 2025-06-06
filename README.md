# Piddiplatsch

## Overview
Piddiplatsch is a [Kafka](https://kafka.apache.org/) consumer for CMIP6+ records that integrates with a [Handle Service](https://pypi.org/project/pyhandle/) for persistent identifiers (PIDs).


## Features
- Listens to a Kafka topic for CMIP6+ records
- Adds, updates, and deletes PIDs in a Handle Service
- Includes a mock Handle Server for testing

## Installation

Prerequisites:
* Use conda/mamba from conda-forge
* https://conda-forge.org/download/ 

Clone the repository from GitHub:
```sh
git clone git@github.com:cehbrecht/piddiplatsch2.git
cd piddiplatsch2
```

Create the conda environment:
```sh
conda env create
conda activate piddiplatsch2
```

Install required packages with:
```sh
pip install -e ".[dev]"

OR

make develop
```

## Configuration

Optionally you can cusomize the default configration to edit the Kafka URL and topic:
```sh
cp src/config/default_config.toml my-config.toml

vim custom-config.toml
```

You can use your customized config for piddiplatsch:
```sh
piddiplatsch --config custom-config.toml
```

## Usage

> ⚠️ **Warning**: You need a running Kafka queue and Handle service. For local testing you can use the provided Docker containers (see below).

> 💡 **Optional**: You can create the Kafka topic if it does not exist yet.
```sh
piddiplatsch init
```

Send a record (json file, STAC format) to the Kafka queue:
```sh
piddiplatsch send -p tests/testdata/CMIP6/CMIP6.ScenarioMIP.MPI-M.MPI-ESM1-2-LR.ssp126.r1i1p1f1.day.tasmin.gn.v20190710.json
```

Start the Kafka consumer to read messages from Kafka queue:
```sh
piddiplatsch consume
```

You can also use the debug mode and a logfile:
```sh
piddiplatsch --debug --logfile consume.log consume
```

## Run tests

Run normal unit tests:
```
make test
```

Run smoke/online tests with an active Kafka queue and Handle service:
```
make smoke
```

## Docker: run Kafka for local testing

Start Kafka with:
```sh
docker-compose up --build -d

OR

make start
```

Stop Kafka with:
```sh
docker-compose down -v

OR

make stop
```

> 💡 **Info**: Docker compose will also start the dummy Handle service for testing.

## Examples

Have a look at the notebooks.

## License

This project is licensed under the Apache License version 2.
