#!/usr/bin/env bash
set -euo pipefail

# Usage: bash scripts/ensure_kafka_topic.sh [CONFIG_PATH] [MAX_ATTEMPTS] [SLEEP_SECS]
# Defaults: CONFIG_PATH=tests/config.toml, MAX_ATTEMPTS=25, SLEEP_SECS=2

CONFIG_PATH=${1:-tests/config.toml}
MAX_ATTEMPTS=${2:-25}
SLEEP_SECS=${3:-2}

echo "Ensuring Kafka topic exists (from config: ${CONFIG_PATH}) ..."

attempt=1
while [ "$attempt" -le "$MAX_ATTEMPTS" ]; do
  python -c 'from piddiplatsch.config import config; config.load_user_config("'"${CONFIG_PATH}"'"); from piddiplatsch.testing.kafka_client import ensure_topic_exists_from_config; ensure_topic_exists_from_config()'
  rc=$?
  if [ "$rc" -eq 0 ]; then
    echo "✅ Kafka topic ensured."
    exit 0
  fi
  echo "⏳ Kafka admin not ready (attempt ${attempt}/${MAX_ATTEMPTS})..."
  sleep "$SLEEP_SECS"
  attempt=$((attempt + 1))
done

echo "⚠️ Proceeding without ensured topic (may fail)."
exit 1
