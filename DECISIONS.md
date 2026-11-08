# Airlytics — Architecture Decision Records

## ADR 000: Free, local-first architecture
- **Status:** Accepted
- **Decision:** The active project must run without paid infrastructure. Core runtime is local Docker + Python: Apache Kafka in KRaft mode, PySpark Structured Streaming, Parquet files, DuckDB, dbt Core/dbt-duckdb, and Streamlit.
- **Reason:** The project is a learning and portfolio system. It must not create cloud bills or require a credit card to demonstrate its core data engineering workflow.
- **Consequences:** The developer's computer supplies compute and disk. The project is not an always-on public service. Throughput and retention are bounded by local hardware and disk space.

## ADR 001: OpenSky as the source
- **Status:** Accepted
- **Decision:** Poll the public OpenSky REST API using the Python ingestion service. Keep polling conservative and respect current API terms, rate limits, and service availability.
- **Consequences:** Live data is dependent on an external service. The project must support replayable local sample fixtures for development and tests.

## ADR 002: Apache Kafka locally
- **Status:** Accepted
- **Decision:** Use Apache Kafka in KRaft mode via Docker Compose. Use a stable aircraft identifier as the message key and retain a dead-letter topic for invalid records.
- **Rejected:** Managed Kafka services and paid hosted brokers. Redpanda is not the active broker so the project can accurately claim Apache Kafka experience.

## ADR 003: PySpark Structured Streaming
- **Status:** Accepted
- **Decision:** Use event-time processing, a 10-minute watermark, and 15-minute inactivity session windows to derive candidate flight legs. Write completed session results to local Parquet with a persistent local checkpoint.
- **Caveat:** Watermarks define how late data is handled; they do not guarantee end-to-end exactly-once delivery. Output idempotency and replay behavior must be tested.

## ADR 004: Local lakehouse and transformations
- **Status:** Accepted
- **Decision:** Store processed records as Parquet and use DuckDB as the local analytical engine. Use dbt Core with dbt-duckdb for staging models, marts, and data tests.
- **Rejected:** BigQuery, Cloud Storage, Dataform, and other managed cloud data services in the active runtime because they can incur charges.

## ADR 005: Dashboard
- **Status:** Accepted
- **Decision:** Streamlit is the presentation layer. It must ultimately query DuckDB marts rather than synthetic data or a separate live API path.
- **Status note:** Existing dashboard provider code is legacy/prototype until its DuckDB integration is completed and tested.

## ADR 006: Infrastructure and automation
- **Status:** Accepted
- **Decision:** Docker Compose manages local services. Terraform files for GCP, Dataflow, Pub/Sub, Cloud Run, and BigQuery are retained as historical learning references only and must not be applied. GitHub Actions is manual-triggered; local tests are the default.
- **Cost guardrail:** No cloud deployment workflow, managed service, billing resource, paid API, or external compute is part of the active project. GitHub-hosted workflow usage remains subject to the account's included allowance.
