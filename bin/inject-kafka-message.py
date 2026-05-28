#!/usr/bin/env python3
"""
Inject a JSON message directly into Kafka for pipeline debugging.

Usage:
    python bin/inject-kafka-message.py debug/sample_snmp_message.json
    python bin/inject-kafka-message.py msg.json --count 1000
    python bin/inject-kafka-message.py msg.json --topic metranova_snmp --count 5000
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Inject a JSON message into Kafka for pipeline debugging"
    )
    parser.add_argument(
        "message_file",
        nargs="?",
        default="-",
        help="Path to a JSON file, or '-' to read from stdin (default: -)",
    )
    parser.add_argument(
        "--topic",
        default="metranova_snmp",
        help="Kafka topic to produce to (default: metranova_snmp)",
    )
    parser.add_argument(
        "--container",
        default="kafka",
        help="Name of the Kafka Docker container (default: kafka)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=1,
        help="Number of times to inject the message (default: 1)",
    )
    args = parser.parse_args()

    # Read the message
    if args.message_file == "-":
        raw = sys.stdin.read().strip()
    else:
        path = Path(args.message_file)
        if not path.exists():
            print(f"Error: file not found: {path}", file=sys.stderr)
            return 1
        raw = path.read_text().strip()

    if not raw:
        print("Error: message is empty", file=sys.stderr)
        return 1

    # Validate JSON before sending
    try:
        json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"Error: invalid JSON: {e}", file=sys.stderr)
        return 1

    # Compact to a single line — kafka-console-producer splits on newlines,
    # so pretty-printed JSON would be sent as broken fragments.
    compact = json.dumps(json.loads(raw), separators=(",", ":"))
    payload = "\n".join([compact] * args.count)
    result = subprocess.run(
        [
            "docker",
            "exec",
            "-i",
            args.container,
            "/opt/kafka/bin/kafka-console-producer.sh",
            "--bootstrap-server",
            "localhost:9092",
            "--topic",
            args.topic,
        ],
        input=payload,
        text=True,
        capture_output=True,
    )

    if result.returncode != 0:
        print(f"Error producing message:\n{result.stderr}", file=sys.stderr)
        return result.returncode

    print(f"Injected {args.count} message(s) into topic '{args.topic}'")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
