"""Consume the topic and write Parquet.

The consumer is a second program on purpose. That is the decoupling: the
puller can finish, crash, or be replaced, and this process still reads the
log from the offset it committed.

    python kafka_bridge/consume.py --out data/kafka/pages.parquet
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from kafka import KafkaConsumer

from fetcher.landing import ARROW_SCHEMA, normalize_record


def main() -> None:
    parser = argparse.ArgumentParser(description="Consume API records from Kafka into Parquet")
    parser.add_argument("--bootstrap", default="localhost:19092")
    parser.add_argument("--topic", default="api-pages")
    parser.add_argument("--group", default="pagination-lab")
    parser.add_argument("--out", default="data/kafka/pages.parquet")
    parser.add_argument(
        "--max-records",
        type=int,
        default=1000,
        help="Stop after this many, so a demo cannot run forever",
    )
    args = parser.parse_args()
    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap,
        group_id=args.group,
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000,
        value_deserializer=lambda raw: __import__("json").loads(raw.decode("utf-8")),
    )
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    writer = pq.ParquetWriter(out, ARROW_SCHEMA, compression="zstd")
    batch: list[dict] = []
    seen = 0
    try:
        for message in consumer:
            batch.append(normalize_record(message.value))
            seen += 1
            if len(batch) >= 500:
                writer.write_table(pa.Table.from_pylist(batch, schema=ARROW_SCHEMA))
                batch = []
                print(f"[kafka] consumed {seen:,}", flush=True)
            if seen >= args.max_records:
                break
        if batch:
            writer.write_table(pa.Table.from_pylist(batch, schema=ARROW_SCHEMA))
    finally:
        writer.close()
        consumer.close()
    print(f"[kafka] wrote {seen:,} records to {out}", flush=True)


if __name__ == "__main__":
    main()
