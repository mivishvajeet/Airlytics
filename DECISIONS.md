# Architecture Decision Log (ADR) - Airlytics

This document captures every architectural decision, context, rationale, and rejected alternatives for the Airlytics real-time flight tracking platform.

---

## ADR 000: Architecture Baseline & Technology Selection
- **Status**: Accepted
- **Context**: Real-time flight tracking requires processing continuous ADS-B position reports from OpenSky Network, transforming them within 2 minutes into flight legs, airport events, and route congestion metrics, with <0.1% duplicates and high cost efficiency on GCP.
- **Decision**: 
  - Standardize on Cloud Run Job for periodic REST polling -> Cloud Pub/Sub for streaming ingestion buffer -> Apache Beam (Dataflow Flex Template) for sessionization and windowed streaming -> BigQuery (partitioned/clustered) for raw & curated marts -> Dataform for incremental SQL transformations -> Terraform for multi-env IaC -> GitHub Actions with Workload Identity Federation (WIF) for zero-key CI/CD.
- **Consequences**:
  - Requires clean separation between stream processing (session state, raw ingestion) and batch/micro-batch ELT (airport geofencing joins, deduplication).
- **Rejected Alternatives**:
  - *Apache Kafka / Confluent Cloud*: Too expensive and high operational overhead for a lean cloud-native GCP portfolio project.
  - *Direct Cloud Run to BigQuery streaming*: Loses the pub/sub buffer and windowing/sessionization capabilities of Apache Beam.
  - *Service Account JSON Keys in GitHub Secrets*: Security vulnerability; rejected in favor of OIDC Workload Identity Federation.

---

## ADR 001: OpenSky Polling & Rate-Limit Resilient Ingestion
- **Status**: Accepted (Phase 1)
- **Context**: OpenSky Network provides real-time ADS-B states via an unauthenticated REST endpoint throttled at 10-second intervals per IP (or 5s with authentication). Requesting global airspace on free tier yields excessive payload sizes and rapid 429 throttling.
- **Decision**:
  - Implement a configurable geographical bounding box default (Western/Central Europe corridor) to limit payload bandwidth and focus on high-density flight operations.
  - Implement exponential backoff with jitter specifically targeting HTTP 429 (Too Many Requests) and 5xx errors.
  - Decouple ingestion from stream processing by publishing normalized state vectors to Google Cloud Pub/Sub with metadata attributes (`icao24`, `event_timestamp`, `on_ground`).
  - Route malformed, out-of-bounds, or unpositioned pings directly to a Dead Letter Queue (DLQ) Pub/Sub topic for auditability.
  - Package poller as a lightweight, non-root Python 3.11 container for Cloud Run Jobs.
- **Consequences**:
  - Cloud Run Job can be triggered by Cloud Scheduler and run for fixed durations or continuous intervals while respecting rate limits.
- **Rejected Alternatives**:
  - *Direct WebSocket connection*: OpenSky does not provide a free raw WebSocket feed for general public use; REST polling is the supported API.
  - *Direct insertion into BigQuery from poller*: Bypassing Pub/Sub removes the buffer required for backpressure and prevents Dataflow from applying session windows across flight pings.

---

## ADR 002: BigQuery Storage Strategy, Partitioning, and Clustering
- **Status**: Accepted (Phase 2)
- **Context**: Flight ping streams generate high write volumes. Uncontrolled table growth or unoptimized scans in BigQuery create substantial cost overhead. Queries frequently filter by single aircraft (`icao24`) or callsign over recent time windows.
- **Decision**:
  - Separate storage into two isolated datasets: `raw_flights` (raw append-only telemetry, late pings, DLQ) and `curated_flights` (sessionized flight legs, takeoff/landing events, route analytics).
  - Partition `raw_flights.raw_pings` daily by `DATE(event_timestamp)` with a 90-day partition expiration policy.
  - Cluster `raw_flights.raw_pings` by `(icao24, callsign)` to allow BigQuery execution engine to prune blocks when transforming flight tracks.
  - Pre-compute BigQuery GIS `GEOGRAPHY` buffers (`ST_BUFFER(airport_point, 5000)`) in `dim_airport` to optimize spatial join performance downstream.
- **Consequences**:
  - Eliminates full-table scan costs for incremental transformations; automatically bounds storage expenditure for personal budget protection.
- **Rejected Alternatives**:
  - *Partitioning by ingestion time*: Event time (`event_timestamp`) reflects actual transponder broadcast time; ingestion time creates time skew during backfills or delayed API responses.
  - *Single combined dataset*: Combining raw and curated layers impedes IAM role separation and table lifecycle management.
