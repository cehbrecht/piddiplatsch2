# Piddiplatsch

## Overview
Piddiplatsch is a Kafka consumer for CMIP7 records that integrates with a Handle Service for persistent identifiers (PIDs).

### Kafka

Docs:

* https://kafka.apache.org/
* https://pypi.org/project/confluent-kafka/

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

or with make:
```sh
make develop 
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

## Use piddiplatsch client

Add a PID record (json file):
```sh
piddiplatsch send -p tests/testdata/CMIP6/CMIP6.ScenarioMIP.MPI-M.MPI-ESM1-2-LR.ssp126.r1i1p1f1.day.tasmin.gn.v20190710.json
```

Consume messages:
```sh
piddiplatsch consume
```

You can also use the debug mode and a logfile:
```sh
piddiplatsch --debug --logfile consume.log consume
```

## Use mock handle service

Start mock handle service:
```sh
python src/piddiplatsch/testing/mock_handle_server.py
```

Check admin user:
```sh
curl -X GET "http://localhost:5000/api/handles/21.T11148/testuser"
```

Register dummy handle:
```sh
curl -X PUT "http://localhost:5000/api/handles/21.T11148/test_1001?overwrite=true" \
  -H "Content-Type: application/json" \
  -d '{
    "values": [
      {
        "index": 1,
        "type": "URL",
        "data": {
          "value": "https://example.org/location/1001"
        }
      }
    ]
  }'
```

Get dummy handle:
```sh
curl -X GET "http://localhost:5000/api/handles/21.T11148/test_1001"
```


