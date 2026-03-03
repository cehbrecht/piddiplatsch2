#!/usr/bin/env python3
"""
Ensure the Kafka topic from a given config exists, with simple retries.

Usage:
  python scripts/ensure_kafka_topic.py [CONFIG_PATH] [--max-attempts N] [--sleep SECS]

Defaults:
  CONFIG_PATH = tests/config.toml
  --max-attempts = 25
  --sleep = 2
"""

from __future__ import annotations

import argparse
import sys
import time

from piddiplatsch.config import config
from piddiplatsch.testing.kafka_client import ensure_topic_exists_from_config


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ensure Kafka topic exists from config")
    p.add_argument(
        "config_path",
        nargs="?",
        default="tests/config.toml",
        help="Path to TOML config file",
    )
    p.add_argument(
        "--max-attempts", type=int, default=25, help="Maximum attempts to ensure topic"
    )
    p.add_argument(
        "--sleep", type=float, default=2.0, help="Seconds to sleep between attempts"
    )
    return p.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)

    print(f"Ensuring Kafka topic exists (from config: {args.config_path}) ...")
    config.load_user_config(args.config_path)

    attempts = max(1, args.max_attempts)
    sleep_secs = max(0.0, args.sleep)

    for i in range(1, attempts + 1):
        try:
            ensure_topic_exists_from_config()
            print("✅ Kafka topic ensured.")
            return 0
        except Exception as e:
            if i < attempts:
                print(f"⏳ Kafka admin not ready (attempt {i}/{attempts})... {e}")
                time.sleep(sleep_secs)
            else:
                break

    print("⚠️ Proceeding without ensured topic (may fail).")
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
