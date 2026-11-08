# Airlytics Migration Plan: Kafka, PySpark Structured Streaming, Dataproc & dbt

## 1. Executive Summary & Review Verdict

This plan captures the architectural migration of Airlytics from Google Cloud native streaming (Pub/Sub + Dataflow) to the industry-standard open-source & cloud hybrid stack:
- **Ingestion**: OpenSky REST API -> Python Producer -> **Apache Kafka** (Local Docker -> GCP)
- **Stream Processing**: **PySpark Structured Streaming** (Watermarking, Sessionization, Deduplication)
- **Compute**: **Google Cloud Dataproc** (Evaluation: Serverless Batches vs. Managed Ephemeral Cluster deferred to Phase 4)
- **Data Lake & Checkpoints**: **Google Cloud Storage (GCS)**
- **Warehouse & Spatial Analytics**: **Google BigQuery** (BigQuery GIS 5km airport geofences)
- **Transformations**: **dbt** (`dbt-bigquery` with staging -> intermediate -> marts)
- **Dashboard**: **Streamlit** (Live airspace map, flight sessions, congestion metrics)
- **IaC & CI/CD**: **Terraform** & **GitHub Actions** (WIF OIDC zero-key deployment)

---

## 2. Phased Execution Roadmap

### Phase 1: Local Kafka & Containerized Producer (Completed ✅)
- [x] Define `docker-compose.yml` with a lightweight Apache Kafka broker (KRaft mode / Redpanda) and Kafka web UI.
- [x] Implement `ingest/kafka_producer.py` with OpenSky polling, JSON schema validation, partitioning by `icao24`, and error handling.
- [x] Add unit tests verifying Kafka serialization and message headers.

### Phase 2: PySpark Structured Streaming Pipeline (Completed ✅)
- [x] Implement `spark/streaming_pipeline.py`:
  - Kafka consumer with schema enforcement (`StructType`).
  - Watermark specification (10 minutes on `event_timestamp`).
  - Session windowing (`session_window("15 minutes")`).
  - Streaming write to BigQuery (`spark-bigquery-connector`) and Parquet / GCS.
  - Streaming recovery and checkpoint directory configuration.
- [x] Add PySpark unit tests with synthetic batch/streaming DataFrames.

### Phase 3: Modern Analytics Engineering with dbt (Completed ✅)
- [x] Initialize `dbt_airlytics/` project with BigQuery adapter.
- [x] Create layered models:
  - `models/staging/stg_raw_pings.sql`
  - `models/marts/dim_airport.sql` (BigQuery GIS `ST_BUFFER`)
  - `models/marts/fact_flight_leg.sql`
  - `models/marts/fact_airport_event.sql` (BigQuery GIS `ST_CONTAINS`)
  - `models/marts/fact_route_agg.sql`
- [x] Configure dbt schema tests, uniqueness, and duplicate rate assertions (<0.1%).

### Phase 4: Google Cloud Dataproc Execution Model (Completed ✅)
- [x] Evaluate streaming continuous execution vs. micro-batch interval execution on GCP.
- [x] Provision Dataproc resources (Serverless vs. Managed Cluster) via Terraform.
- [x] Configure GCS checkpoint buckets and IAM permissions.

### Phase 5: CI/CD & Production Hardening (Completed ✅)
- [x] GitHub Actions pipeline for PySpark unit tests, dbt compilation/testing, and Terraform validation.
- [x] End-to-end local and cloud verification run.

