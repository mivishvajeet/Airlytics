# Airlytics Local Development Runbook

## Cost policy
This runbook uses local Docker containers and local Python processes only. Do not configure cloud credentials or run Terraform. The active workflow has no required paid service. OpenSky is an external public API and remains subject to its current rate limits and terms.

## Requirements
- Docker Desktop / Docker Engine with Compose
- Python 3.11+
- Java 17+
- Git

## Start and stop Kafka

```bash
docker compose up -d kafka kafka-ui
docker compose ps
docker compose logs -f kafka
docker compose down
```

Kafka bootstrap address from the host: `localhost:19092`.
Kafka UI: http://localhost:8080.

## Python environment

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r ingest/requirements.txt
pip install -r spark/requirements.txt
```

Set local environment values:

```bash
export STREAMING_SINK=kafka
export KAFKA_BOOTSTRAP_SERVERS=localhost:19092
export KAFKA_TOPIC=raw-flight-pings
export KAFKA_DEADLETTER_TOPIC=deadletter-pings
```

Start ingestion:

```bash
python -m ingest.main
```

Start Spark in another terminal:

```bash
PYTHONPATH=. spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  spark/streaming_pipeline.py \
  --kafka-bootstrap localhost:19092 \
  --topic raw-flight-pings \
  --starting-offsets earliest \
  --output-path data/bronze/flight-legs \
  --checkpoint-dir data/checkpoints/flight-legs
```

## dbt / DuckDB

Install the adapter and create the local profile:

```bash
pip install dbt-core dbt-duckdb
mkdir -p ~/.dbt warehouse
cp dbt_airlytics/profiles.yml.example ~/.dbt/profiles.yml
```

Run `dbt debug --project-dir dbt_airlytics` before `dbt build --project-dir dbt_airlytics`. dbt models must point at the actual Parquet output schema and location; validate those paths against a real Spark run before treating marts as production-ready.

## Shutdown and cleanup

```bash
docker compose down
```

To also delete Kafka's local persisted data:

```bash
docker compose down -v
```

The last command is destructive for the Kafka volume. It does not delete `data/` or `warehouse/`; remove those directories manually only if you intend to delete local datasets and checkpoints.
