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

---

## ADR 003: Apache Beam Windowing, Sessionization, and Late Data Strategy
- **Status**: Accepted (Phase 3)
- **Context**: Flight positions are discrete, irregular pings. Identifying distinct flight legs (gate-to-gate or takeoff-to-touchdown) requires detecting silence periods when the transponder is off or out of coverage, while handling network delays and late arrivals.
- **Decision**:
  - Implement gap-based session windowing (`window.Sessions(gap_size=15 minutes)`) keyed by `icao24` to delineate flight legs.
  - Configure allowed lateness (10 minutes) with accumulation mode `DISCARDING` and route late events to a dedicated BigQuery sink (`raw_flights.late_pings`) via a tagged side output.
  - Compute deterministic surrogate keys for each flight leg (`MD5(icao24 || start_iso)`), ensuring idempotency if sessions are re-evaluated.
  - Package pipeline as a Google Cloud Dataflow Flex Template with maximum worker cap hardcoded to 2 (`max_num_workers = 2`) for personal project budget safety.
- **Consequences**:
  - Automatically splits turnaround operations at airport gates without requiring manual flight schedule inputs.
- **Rejected Alternatives**:
  - *Fixed 1-hour tumbling windows*: Arbitrarily slices flight legs across window boundaries, corrupting origin, destination, and duration statistics.
  - *Dropping late data silently*: Prevents auditability of streaming lag and introduces telemetry blind spots; rejected in favor of side-output landing into `raw_flights.late_pings`.

---

## ADR 004: Dataform Incremental ELT, GIS Geofencing, and Quality Assertions
- **Status**: Accepted (Phase 4)
- **Context**: Raw pings must be enriched with airport identities, takeoff/landing event types, and route statistics. Running full-table scans over millions of pings is prohibitively expensive and violates the <0.1% duplicate threshold.
- **Decision**:
  - Implement incremental Dataform SQLX models utilizing `${when(incremental(), ...)}` predicates bounded by `event_timestamp` watermarks.
  - Pre-generate geodesic 5km buffer polygons (`ST_BUFFER(airport_point, 5000.0)`) in `dim_airport` to enable spatial containment joins via `ST_CONTAINS(a.geofence_polygon, p.ping_point)` for takeoff and landing event detection.
  - Detect takeoff vs. landing events using aircraft vertical speed kinematics (`vertical_rate >= 1.5 m/s` for takeoff, `<= -1.5 m/s` for landing) within airport vicinity.
  - Enforce automated quality assertions:
    - Deduplication assertion fails if duplicate rate in `fact_airport_event` exceeds 0.1%.
    - Flight duration assertion ensures durations are positive and <= 24 hours.
    - Airspace freshness assertion flags any ingestion delay exceeding 10 minutes.
- **Consequences**:
  - Incremental execution processes only newly arrived partitions, guaranteeing sub-linear query costs.
- **Rejected Alternatives**:
  - *Dataflow-only spatial joins*: Geofencing 70,000 global airports in Apache Beam workers requires heavy memory footprints or external API calls; BigQuery GIS natively evaluates spatial indexes efficiently in SQL.
  - *Manual BigQuery scheduled queries*: Lacks version control, dependency DAG resolution, assertion testing, and CI/CD compiler validation provided by Dataform.

---

## ADR 005: Multi-Environment Infrastructure as Code & Cost Guardrails
- **Status**: Accepted (Phase 5)
- **Context**: Operating a streaming architecture with Cloud Run, Pub/Sub, Dataflow, and BigQuery requires strict infrastructure automation without hardcoded secrets or uncontrolled personal billing surprises.
- **Decision**:
  - Adopt a modular Terraform architecture (`infra/modules/`) separated into isolated `dev` and `prod` environment workspaces.
  - Parameterize all GCP project IDs using default dummy IDs (`airlytics-dev-123456`, `airlytics-prod-123456`) to ensure portability and zero secret leakage.
  - Enforce personal cost budget constraints:
    - Set GCS bucket lifecycle policies to automatically purge Dataflow staging/temp files after 7 days.
    - Declare a Google Cloud Billing Budget resource with spending alerts triggered at 50%, 80%, and 100% of threshold ($25 dev / $50 prod).
    - Cap Dataflow max workers to 2 (`max_num_workers = 2`).
    - Schedule Cloud Run ingestion jobs via Cloud Scheduler during active operating windows.
  - Implement a 10-minute metric absence alert policy in Cloud Monitoring (`duration = "600s"`) to satisfy the system freshness requirement.
- **Consequences**:
  - Completely reproducible environments with zero manual console configuration.
- **Rejected Alternatives**:
  - *Terraform Workspaces in a single directory*: Confining dev and prod state to workspace flags often leads to accidental cross-environment applies; discrete directory structures (`infra/dev`, `infra/prod`) provide hard isolation.
  - *Manual GCS bucket and topic creation*: Violates the immutable infrastructure and CI/CD automation requirement.

---

## ADR 006: Zero-Key CI/CD via Workload Identity Federation (WIF) and Approval Gates
- **Status**: Accepted (Phase 6)
- **Context**: Automated deployments require GCP credentials with elevated permissions (Terraform, Dataflow, Cloud Run). Exporting service account JSON keys to GitHub Secrets poses severe security risks (key rotation burden, credential leakage).
- **Decision**:
  - Implement Workload Identity Federation (WIF) using short-lived OpenID Connect (OIDC) JWT tokens issued by GitHub Actions.
  - Structure the CI pipeline on Pull Requests to execute flake8, pytest with synthetic Beam fixtures, `terraform fmt -check`, `terraform validate`, and Dataform DAG compilation.
  - Structure the CD pipeline on merge to `main` with a 2-stage promotion:
    - Automatically apply Terraform and deploy the Dataflow Flex Template to `dev`.
    - Enforce a manual GitHub Environment approval gate for `production` before deploying to `prod`.
  - Strictly prohibit service account JSON keys across the entire codebase and secret storage.
- **Consequences**:
  - Cloud credentials expire automatically within minutes; zero persistent keys exist to be leaked.
- **Rejected Alternatives**:
  - *Storing Service Account JSON in GitHub Secrets*: Static, unexpiring keys; strictly rejected.
  - *Direct manual `terraform apply` or `gcloud run deploy` from local workstation*: Bypasses peer review, automated testing, and reproducibility audits.
