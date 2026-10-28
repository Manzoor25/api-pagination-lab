"""Publish already-landed pages to a Kafka topic.

What this file teaches
----------------------
Pulling from an API and pushing onto a stream solve different problems.

- Pull (pagination): you ask, on your schedule, and you are responsible for
  not missing a page.
- Push (Kafka): producers append records. Many consumers can read the same
  topic, and they can replay it.

Land API data in Kafka when several downstream jobs need the same pull, when
you want replay, or when you want the puller to be decoupled from the
warehouse load. It is overkill when one script writes one Parquet file and
nobody else is listening.

This script reads NDJSON pages that fetch.py already wrote. It does not pull
from the API. Install the extra dependency first:

    pip install -r requirements-kafka.txt

Then:

    python kafka_bridge/publish.py --run-dir data/landing/keyset/<run-id>
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from kafka import KafkaProducer


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish landed pages to Kafka/Redpanda")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--bootstrap", default="localhost:19092")
    parser.add_argument("--topic", default="api-pages")
    args = parser.parse_args()
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        linger_ms=50,
        acks="all",
    )
    sent = 0
    page_dir = Path(args.run_dir) / "pages"
    for path in sorted(page_dir.glob("page_*.ndjson")):
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line:
                continue
            producer.send(args.topic, json.loads(line))
            sent += 1
            if sent % 5000 == 0:
                print(f"[kafka] sent {sent:,}", flush=True)
    producer.flush()
    producer.close()
    print(f"[kafka] published {sent:,} records to {args.topic}", flush=True)


if __name__ == "__main__":
    main()
