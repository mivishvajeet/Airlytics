# 02 — File-by-file code guide

This guide follows the repository's current layout on `migration/free-local-stack`. It describes the code that exists and calls out mismatches that still need work. It is not a claim that every component has passed an end-to-end run.

## Repository map

| Path | Responsibility | Current status |
|---|---|---|
| `ingest/` | OpenSky polling, parsing, normalization, Kafka publishing | Main local ingestion path exists; validate end to end |
| `spark/` | Kafka consumption and streaming transformations | PySpark pipeline exists; validate connector/runtime |
| `data/` | Local Parquet and checkpoints at runtime | Generated locally; should be git-ignored |
| `dbt_airlytics/` | SQL transformation project | Project/profile scaffold exists; SQL models remain BigQuery-specific |
| `dashboard/` | Streamlit UI and data-provider calculations | UI exists; provider currently uses a mismatched poller method and synthetic fallback |
| `docker-compose.yml` | Local Kafka broker and Kafka UI | Apache Kafka KRaft service configured |
| `.env.example` | Example local environment values | Example only; copy to a local env file if the runner loads it |
| `Makefile` | Convenience commands | Includes start, stop, logs, status, clean, test |
| `.github/workflows/` | Manual GitHub Actions checks / disabled deployment | Manual only; CI runs ingestion tests |
| `infra/`, `dataflow/`, `dataform/` | Earlier cloud architecture/reference material | Legacy; not part of supported local runtime |

## Ingestion modules

### `ingest/main.py`
The executable entry point. It creates `IngestionConfig`, `OpenSkyPoller`, and `KafkaPingPublisher` through `IngestionRunner`. The loop calls `poll_cycle()`, publishes valid pings to the primary topic and rejected records to the dead-letter topic, logs cycle totals, sleeps to maintain the configured polling interval, and exits on SIGINT/SIGTERM or a configured run duration.

Important details:
- `STREAMING_SINK` must be `kafka`; other values deliberately raise an error.
- `DRY_RUN` is read in `main()` and passed to the publisher.
- The module docstring and some signal comments still mention Cloud Run. Those are stale comments, not the active deployment design.
- A graceful process exit does not itself prove every Kafka message was acknowledged; inspect publisher delivery handling.

### `ingest/config.py`
A frozen dataclass that reads environment variables. It centralizes the API URL, optional credentials, poll interval, timeout, retry/backoff values, bounding box, Kafka bootstrap server/topics, and bounded-run duration.

Defaults currently include:
- Kafka from the host: `localhost:19092`
- Main topic: `raw-flight-pings`
- Dead-letter topic: `deadletter-pings`
- Poll interval: 10 seconds
- Request timeout: 15 seconds
- Maximum retries: 3
- Bounding box: latitude 45–55, longitude -5–15

The bounding box is a default example region, not a global feed. The class also retains legacy GCP/Pub/Sub fields. They are not used by the Kafka-only ingestion runner and should be removed only after confirming no old scripts/tests depend on them.

### `ingest/poller.py`
Owns HTTP communication with OpenSky and coordinates parsing. It builds optional bounding-box parameters, applies authentication when configured, measures request latency, interprets the response and produces a `PollResult` containing the API response time, count of states, valid normalized pings, dead-letter records, latency, and status code.

Review the actual retry branches before changing them. Reliability questions to test include:
- Are 429 responses handled with a longer delay than transient 5xx/network failures?
- Is exponential backoff capped and jittered?
- Is the `Retry-After` header respected?
- Does a successful HTTP 200 with an empty `states` array remain a successful empty poll rather than being treated as an outage?
- Are malformed JSON and timeouts converted into explicit poll outcomes?
- Is the request timeout applied in every HTTP implementation path?

### `ingest/models.py`
Defines the canonical normalized ping model and parsing from OpenSky's positional state-vector array. Named fields make downstream code safer than repeatedly using numeric array offsets.

OpenSky state vectors can contain null optional values. The parser should validate the aircraft identifier and usable position, preserve nullable telemetry, and route invalid records to the DLQ rather than silently inventing values.

Do not assume similarly named fields are interchangeable. The dashboard currently references attributes such as `ping_id` and `altitude_baro_m`; confirm the dataclass's actual field names before integrating it. A stable event/message ID should be explicitly defined if deterministic deduplication is required.

### `ingest/kafka_producer.py`
Serializes normalized pings to JSON and sends them to Kafka. The aircraft identifier (`icao24`) is used as the message key, which gives partition affinity for one aircraft. Valid pings and rejected records go to separate topics.

Validate producer settings and delivery semantics:
- `acks=all` asks the broker to acknowledge according to the topic's in-sync replica policy.
- Retries can help with transient broker errors.
- A successful local `send()` enqueue is not always the same as confirmed delivery; inspect futures/callbacks and flush/close behavior.
- The local Compose cluster is a single broker, so replication-factor guarantees are not equivalent to a multi-broker production cluster.

### `ingest/requirements.txt` and `ingest/tests/`
The requirements file contains the Python ingestion dependencies. Tests cover parsing and poller behavior with controlled inputs/mocks. Unit tests do not prove that a real OpenSky request, Kafka broker, Spark job, and Parquet sink work together.

## Streaming modules

### `spark/schemas.py`
Defines the Spark schema used to parse JSON values from Kafka. An explicit schema avoids repeatedly inferring field types and makes nullable fields visible. Keep it aligned with the exact serialized dataclass field names and types.

### `spark/streaming_pipeline.py`
The intended processing path is:
1. Create a Spark session.
2. Read Kafka topic values as a streaming DataFrame.
3. Parse JSON using the declared schema.
4. convert event-time fields to Spark timestamps.
5. Apply a watermark to bound state for late data.
6. Group by aircraft and a session window based on inactivity.
7. Aggregate ping count, start/end time, duration, maximum barometric altitude and average velocity.
8. Write completed sessions to Parquet in append mode with a checkpoint directory.

The current design uses a 10-minute watermark and a 15-minute inactivity gap (verify the constants in source before changing). A session is an observed telemetry session, not proof of a commercial flight's true origin/destination or gate-to-gate journey.

The Kafka Spark connector is a JVM package, not just a Python pip dependency. The `spark-submit --packages` or equivalent runtime must use a connector version compatible with the installed Spark/Scala versions.

### `spark/requirements.txt` and `spark/tests/`
Python dependencies include PySpark and Chispa. Chispa helps compare Spark DataFrames in tests. Confirm that Spark tests can start in the target local Java/Spark environment.

## Analytics and presentation

### `dbt_airlytics/dbt_project.yml`
Declares the dbt project, model directories, and default materializations. Staging models are configured as views and marts as tables, with schema names configured per layer.

### `dbt_airlytics/profiles.yml.example`
Provides a local DuckDB profile example. dbt requires the `dbt-duckdb` adapter in the environment. Verify the relative DuckDB file path from the directory where dbt is invoked.

### `models/staging/stg_raw_pings.sql`
Intended to cast, standardize, filter and deduplicate raw ping records. It currently contains BigQuery-specific SQL such as `INT64`, `FLOAT64`, `ST_GEOGPOINT`, and `QUALIFY`. It also expects a dbt source named `raw_flights.raw_pings`. It is not yet a verified DuckDB model.

### `models/marts/fact_flight_leg.sql`
Intended to derive aircraft sessions using `LAG`, a 15-minute gap rule, cumulative session numbering, and aggregates. Its current SQL uses BigQuery-specific configuration/functions including partition/cluster configuration, `TIMESTAMP_DIFF`, and `UNIX_SECONDS`. It is not yet a verified DuckDB model. The Spark pipeline also produces session aggregates, so decide whether Spark or dbt owns the authoritative flight-session mart to avoid two conflicting definitions.

### `dashboard/app.py`
Builds the Streamlit presentation: page settings, theme/CSS, KPI calculations, tabs/charts and layout. It calls the dashboard provider and derives flight-leg, airport-event and route-level views. The UI should eventually read persisted analytical data rather than silently substituting synthetic records.

### `dashboard/data_provider.py`
Contains OpenSky retrieval, synthetic sample generation, distance calculations, nearest-hub-airport lookup and derived dashboard datasets. Current static review found that it calls `poll_once()`, while the poller entry point used by ingestion is `poll_cycle()`. It also references ping attributes that may not match the normalized dataclass. The dashboard catches live-fetch exceptions and falls back to synthetic data, so a visually populated screen does not prove that live data was loaded.

## Local operations

### `docker-compose.yml`
Starts Apache Kafka 3.9.0 in KRaft mode and Kafka UI. Host applications connect to `localhost:19092`; other Compose containers connect to `kafka:9092`. A named volume persists Kafka data. Kafka UI is exposed on port 8080.

### `.env.example`
Documents local variables. It is not automatically loaded by every command. Confirm whether the launcher uses python-dotenv, Compose `env_file`, or shell exports. Never commit real credentials.

### `Makefile`
- `make up`: starts Kafka and Kafka UI.
- `make down`: stops services but preserves named volumes.
- `make logs`: follows service logs.
- `make ps`: lists Compose services.
- `make clean`: runs `docker compose down -v` and deletes named volumes. **This removes local Kafka data.**
- `make test`: runs pytest across the listed test paths.

### GitHub Actions
`.github/workflows/ci.yml` is manual-triggered and runs ingestion tests on a GitHub-hosted runner. `.github/workflows/cd.yml` is a manual safety notice and performs no cloud deployment. Manual workflows can still consume GitHub Actions minutes when run; do not enable frequent schedules if the zero-cost constraint is strict.

## Legacy directories
The earlier cloud architecture contains Terraform, Beam/Dataflow, Pub/Sub and Dataform resources. Treat them as reference/legacy until they are deliberately removed. Do not run Terraform apply or deploy cloud resources as part of the local workflow. Keep the README and docs explicit about what is active versus historical.
