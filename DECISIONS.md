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
