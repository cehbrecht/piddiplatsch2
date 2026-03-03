#!/usr/bin/env bash
set -euo pipefail

# Wait for Kafka brokers' Docker healthchecks to report healthy.
# Usage: wait_for_kafka_health.sh [RETRIES] [SLEEP_SECONDS]
# Defaults: RETRIES=30, SLEEP_SECONDS=2

RETRIES=${1:-30}
SLEEP_SECONDS=${2:-2}
SERVICES=("kafka1" "kafka2" "kafka3")

echo "🔍 Waiting for Kafka broker healthchecks..."

for ((i=0; i<RETRIES; i++)); do
  unhealthy=()
  for s in "${SERVICES[@]}"; do
    status=$(docker inspect --format '{{.State.Health.Status}}' "$s" 2>/dev/null || echo "unknown")
    if [[ "$status" != "healthy" ]]; then
      unhealthy+=("$s($status)")
    fi
  done

  if [[ ${#unhealthy[@]} -eq 0 ]]; then
    echo "✅ All Kafka brokers healthy!"
    exit 0
  fi

  echo "⏳ Waiting on: ${unhealthy[*]}"
  sleep "$SLEEP_SECONDS"
done

echo "❌ Brokers not healthy after timeout."
exit 1
