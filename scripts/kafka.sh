#!/bin/bash

KAFKA_CONTAINER="kafka"
KAFKA_BROKER="localhost:9092"

case "$1" in
  list)
    docker exec -it "$KAFKA_CONTAINER" kafka-topics.sh --bootstrap-server "$KAFKA_BROKER" --list
    ;;
  create)
    if [ -z "$2" ]; then
      echo "Usage: $0 create <topic-name>"
      exit 1
    fi
    docker exec -it "$KAFKA_CONTAINER" kafka-topics.sh --bootstrap-server "$KAFKA_BROKER" --create --topic "$2" --partitions 1 --replication-factor 1
    ;;
  consume)
    if [ -z "$2" ]; then
      echo "Usage: $0 consume <topic-name>"
      exit 1
    fi
    docker exec -it "$KAFKA_CONTAINER" kafka-console-consumer.sh --bootstrap-server "$KAFKA_BROKER" --topic "$2" --from-beginning
    ;;
  send)
    if [ -z "$2" ] || [ -z "$3" ]; then
      echo "Usage: $0 send <topic-name> <message>"
      exit 1
    fi
    echo "$3" | docker exec -i "$KAFKA_CONTAINER" kafka-console-producer.sh --topic "$2" --bootstrap-server "$KAFKA_BROKER"
    ;;
  *)
    echo "Usage: $0 {list|create <topic-name>|consume <topic-name>}"
    exit 1
    ;;
esac
