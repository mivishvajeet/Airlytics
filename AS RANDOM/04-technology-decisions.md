# 04 — Technology decisions and alternatives

## Decision principles

Airlytics has one hard constraint: the core project must run locally without requiring paid cloud infrastructure. Tool selection should be justified by the learning objective, operational complexity, and evidence that the tool is actually used.

## Current target stack

| Layer | Selected tool | Why it fits this project | Alternatives and trade-offs |
|---|---|---|---|
| Source | OpenSky REST API | Real external telemetry source | Historical CSV/fixtures are easier and deterministic but less live |
| Ingestion | Python | Good HTTP, parsing, testing and ecosystem support | Go/Java can offer strong concurrency but add learning/setup cost |
| Event transport | Apache Kafka (KRaft) | Widely used distributed event log; teaches topics, keys, partitions, offsets | RabbitMQ is queue-oriented; Redis Streams is lighter; Pub/Sub is managed cloud and violates local-only requirement |
| Stream processing | PySpark Structured Streaming | Familiar Spark DataFrame model and event-time/stateful operations | Apache Beam is portable; Flink is strong for low-latency streaming; Kafka Streams is JVM-centric |
| File storage | Parquet | Open columnar format; interoperable with Spark and DuckDB | CSV is simple but weakly typed and inefficient; JSON is flexible but less analytical |
| Local analytics | DuckDB | Embedded OLAP engine; directly queries Parquet | PostgreSQL is a server-based relational database; SQLite is not optimized for analytical scans |
| SQL transformations | dbt Core + dbt-duckdb | Version-controlled SQL models, dependencies and tests | Dataform is GCP-oriented; plain SQL scripts are simpler but lack dbt's model graph/testing conventions |
| Dashboard | Streamlit | Fast Python-native data app | Superset/Metabase offer richer BI but add server/database setup |
| Local orchestration | Docker Compose | Reproducible multi-container local services | Kubernetes is excessive for a single-developer learning environment |
| CI | GitHub Actions, manual trigger | Repeatable test runner | Local pre-commit/pytest is free of hosted minutes but less visible to reviewers |

## Why Kafka instead of Pub/Sub?

Both move events between producers and consumers, but they differ operationally. Pub/Sub is a managed Google Cloud messaging service. Kafka is a broker platform that can run locally and exposes topics, partitions, offsets, retention and consumer groups directly.

The local-first requirement makes Kafka the active choice. Pub/Sub remains legacy/reference material only. Do not run both as active transports for the same pipeline without a specific dual-write requirement; it increases complexity and creates consistency problems.

## Why PySpark instead of Beam/Dataflow?

The old architecture used Apache Beam with Google Cloud Dataflow. That is a valid managed-cloud architecture, but it requires cloud infrastructure and billing configuration. The new local target uses Spark Structured Streaming to learn Spark DataFrames, event time, watermarks, stateful aggregation and Parquet output without deploying Dataflow.

Beam is runner-portable and Dataflow is a managed runner. Spark is a separate processing engine. They are not interchangeable APIs, and this repository should not claim it runs Beam/Dataflow in the local path.

## Why dbt instead of Dataform?

Dataform is integrated with BigQuery workflows. dbt Core with DuckDB supports a local SQL transformation workflow. The project should keep one authoritative transformation layer. If Spark owns sessionization, dbt should build downstream analytical marts rather than independently re-implementing the same flight-session logic unless there is a deliberate comparison exercise.

## Why Parquet instead of writing only to a database?

Parquet creates portable analytical files and separates stream processing from query execution. DuckDB can query the files directly, which keeps the local system lightweight. A database table may still be useful for serving repeated dashboard queries, but it is not required for the first local version.

## Why DuckDB instead of PostgreSQL?

The main workload is analytical: scans, aggregates, windows and dashboard queries. DuckDB runs in-process and can query Parquet with little operational overhead. PostgreSQL is a better fit when the application needs concurrent transactional writes, multi-user serving, row-level updates, or a central shared database. It is possible to use both, but that should solve a real need rather than add infrastructure for its own sake.

## Why Streamlit?

Streamlit makes a Python DataFrame or SQL result visible quickly. It is appropriate for a portfolio analytics dashboard, not a replacement for a production front-end architecture. Keep business logic in provider/query modules rather than embedding all transformations in UI code.

## Why Docker Compose instead of Kubernetes?

Two local services (Kafka and Kafka UI) do not justify Kubernetes. Compose is easier to understand, starts quickly and is sufficient to reproduce the broker environment. Kubernetes becomes relevant when learning scheduling, service discovery, scaling, rolling deployments and multi-service orchestration.

## What does not belong in the free local runtime?

- GCP Pub/Sub, Dataflow, BigQuery, Dataproc, Cloud Run and Cloud Storage.
- Terraform apply or any cloud provisioning.
- Managed Kafka services.
- Paid API tiers or paid observability platforms.
- Hosted runners on frequent schedules if they could exceed included GitHub Actions allowances.

The old Terraform, Dataflow, Dataform and Pub/Sub directories can remain as historical reference while the migration is in progress. Label them clearly and never mistake their presence for active runtime dependencies.

## Compatibility decisions to lock down

1. Pin compatible Spark, Scala and Kafka connector versions.
2. Keep the Python Kafka client and Spark Kafka connector roles distinct.
3. Add `dbt-duckdb` to the dbt environment and port BigQuery SQL.
4. Agree on one canonical ping schema and version it.
5. Decide whether Spark or dbt owns sessionization.
6. Define where raw events and curated sessions live; avoid calling derived flight sessions `bronze`.
7. Make dashboard queries read persisted data and display live/synthetic source status honestly.

## Interview answer framework

When asked "Why this stack?", answer in this order:
1. Requirement: real-time-ish telemetry, reproducible local execution, no paid cloud.
2. Design: Python ingestion -> Kafka -> Spark -> Parquet -> DuckDB/dbt -> Streamlit.
3. Trade-off: local single-broker Kafka is not highly available; Spark has higher local resource needs than a small Python consumer.
4. Evidence: name the tests and commands that you personally ran, with actual results.
5. Next step: explain the one most important unresolved integration gap rather than claiming production readiness.
