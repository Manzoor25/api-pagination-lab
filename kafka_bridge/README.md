# Optional Kafka bridge

This directory is off by default. Nothing else in the lab starts Docker.

## Pull versus push

An API pull is you asking. You choose when, you page, and you have to prove you did not skip a row.

A stream is the producer pushing. Kafka (here, Redpanda, which speaks the Kafka protocol) keeps an append-only log. Several consumers can read it. A consumer that crashes can replay from the offset it committed.

Put API data into Kafka when:

- more than one job needs the same pull
- you want replay without hitting the API again
- the team that pages the API is not the team that loads the warehouse

Skip Kafka when one script writes one Parquet file and that is the whole pipeline. A topic, a broker, and a consumer group are extra moving parts. They do not make the API faster.

## Run it

```bash
docker compose -f kafka_bridge/docker-compose.yml up -d
pip install -r requirements-kafka.txt
python kafka_bridge/publish.py --run-dir data/landing/keyset/<run-id>
python kafka_bridge/consume.py --out data/kafka/pages.parquet --max-records 1000
```

The broker listens on `localhost:19092`.
