# Airlytics ✈️
### Real-Time Flight Tracking & Airspace Analytics Platform on Google Cloud

[![CI](https://github.com/mivishvajeet/Airlytics/actions/workflows/ci.yml/badge.svg)](https://github.com/mivishvajeet/Airlytics/actions/workflows/ci.yml)
[![CD](https://github.com/mivishvajeet/Airlytics/actions/workflows/cd.yml/badge.svg)](https://github.com/mivishvajeet/Airlytics/actions/workflows/cd.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.11%2B-brightgreen.svg)](https://www.python.org/)
[![Apache Beam](https://img.shields.io/badge/Apache%20Beam-2.50%2B-orange.svg)](https://beam.apache.org/)
[![GCP Dataflow](https://img.shields.io/badge/GCP-Dataflow%20Flex%20Template-4285F4.svg)](https://cloud.google.com/dataflow)
[![BigQuery GIS](https://img.shields.io/badge/BigQuery-GIS%20%26%20Dataform-669DF6.svg)](https://cloud.google.com/bigquery)
[![Terraform](https://img.shields.io/badge/IaC-Terraform%201.5%2B-purple.svg)](https://www.terraform.io/)

Airlytics is a production-grade, portfolio-quality real-time streaming data platform built on Google Cloud. It ingests continuous ADS-B flight transponder telemetry from the [OpenSky Network REST API](https://opensky-network.org/), performs stateful sessionization and windowed streaming with Apache Beam on Cloud Dataflow, and derives decision-ready dimensional marts, spatial geofences, and route metrics using BigQuery GIS and Dataform.

---

## 1. Problem Statement & Business Questions

Raw aviation telemetry arrives as unstructured, high-frequency position reports (`icao24`, `callsign`, `lat`, `lon`, `altitude`, `velocity`, `timestamp`). Individual pings answer zero operational business questions in isolation. Airlytics transforms this continuous stream within **under 2 minutes** of event capture into high-fidelity decision analytics:

1. **Airspace Situational Awareness**: Which aircraft are airborne right now vs. on ground?
2. **Flight Leg Reconstruction**: What discrete flight legs were completed (origin, destination, duration, max altitude, average speed)?
3. **Airport Congestion & Geofencing**: Which airports experienced the highest volume of departures/arrivals per hour? Are there congestion spikes?
4. **Route Performance & Variance**: What are the busiest airline routes, and how do flight durations and altitudes vary across operating windows?

---

## 2. Architecture & Data Flow

```mermaid
flowchart LR
    subgraph Ingestion ["Ingestion (Cloud Run & Pub/Sub)"]
        OpenSky["OpenSky Network REST API"] -->|10-15s Polling with Backoff| CloudRun["Cloud Run Job (Python 3.11)"]
        CloudRun -->|JSON Pings| PubSub["Pub/Sub Topic: raw-flight-pings"]
        CloudRun -.->|Malformed / Out-of-bounds| DLQ["Pub/Sub Topic: deadletter-pings"]
    end

    subgraph Streaming ["Streaming Engine (Google Cloud Dataflow)"]
        PubSub --> Beam["Dataflow Flex Template (Beam Streaming)"]
        Beam -->|Direct Append| BQRaw["BigQuery: raw_flights.raw_pings"]
        Beam -->|15-Min Gap Session Windows| FlightLegs["fact_flight_leg Candidates"]
        Beam -->|Late Data (>10m)| BQLate["BigQuery: raw_flights.late_pings"]
        Beam -->|Corrupt Payloads| BQDLQ["BigQuery: raw_flights.deadletter_pings"]
    end

    subgraph Transformation ["ELT & Curated Marts (Dataform & BQ GIS)"]
        OurAirports["OurAirports Open Data (GCS)"] --> DimAirport["curated_flights.dim_airport (GIS 5km Buffers)"]
        BQRaw & FlightLegs --> Dataform["Dataform Incremental SQLX Models"]
        Dataform --> CuratedLegs["curated_flights.fact_flight_leg"]
        Dataform --> CuratedEvents["curated_flights.fact_airport_event (ST_CONTAINS)"]
        Dataform --> RouteAgg["curated_flights.fact_route_agg"]
        Dataform --> DimAircraft["curated_flights.dim_aircraft"]
    end

    subgraph Governance ["Governance, Observability & CI/CD"]
        Freshness["Cloud Monitoring: Freshness Alert (10m silence)"]
        Budget["GCP Billing Budget Alert ($25 dev / $50 prod)"]
        WIF["GitHub Actions CI/CD via Workload Identity Federation"]
    end
```

---

## 3. Key Technical Highlights & Success Criteria

| Requirement | Implementation Architecture | Target SLA / Metric | Status |
| :--- | :--- | :--- | :--- |
| **End-to-End Latency** | Streaming Pub/Sub buffer + Dataflow streaming write | < 2 minutes ping-to-queryable | ✅ Passed |
| **Deduplication** | Unique surrogate keys (`MD5(icao24 \|\| timestamp)`) + Dataform incremental merges | < 0.1% duplicate rate | ✅ Enforced via assertion |
| **Freshness Alerting** | Cloud Monitoring metric absence alert policy (`duration = "600s"`) | Alert triggered after 10m silence | ✅ Declared in Terraform |
| **Cost Guardrails** | Dataflow workers capped to 2 (`max_num_workers = 2`), scheduled ingestion hours, GCS 7-day temp lifecycle, GCP Billing alerts | Personal budget protection ($25/mo) | ✅ Enforced in IaC & Beam |
| **Security & Zero Keys** | Workload Identity Federation (WIF) OIDC authentication | Strictly 0 static SA JSON keys | ✅ Enforced in CI/CD |
| **Deployment Automation**| Multi-stage GitHub Actions with GitHub Environment approval gates | Deployments only via CI/CD | ✅ Automated |

---

## 4. Curated Data Models

All BigQuery tables are partitioned and clustered to minimize scan costs:

- **`curated_flights.dim_airport`**: Seeded from the OurAirports open dataset. Contains airport metadata, ICAO/IATA identifiers, elevation, `airport_point` (`GEOGRAPHY`), and pre-calculated 5km buffer `geofence_polygon` (`GEOGRAPHY`).
- **`curated_flights.dim_aircraft`**: Incremental aircraft registry capturing `icao24`, latest observed callsign, origin country, first seen, last seen, and total pings observed.
- **`curated_flights.fact_flight_leg`**: Individual flight legs sessionized via gap-based windowing (15 minutes). Features resolved origin and destination airports (within 10km), flight duration, max altitude, and average velocity.
- **`curated_flights.fact_airport_event`**: Takeoff and landing events resolved via BigQuery GIS spatial containment (`ST_CONTAINS(a.geofence_polygon, p.ping_point)`) and vertical rate kinematics (`vertical_rate >= 1.5 m/s` vs `<= -1.5 m/s`).
- **`curated_flights.fact_route_agg`**: Hourly aggregations for distinct origin-to-destination pairs tracking flight volumes, average durations, and duration min/max bounds.

---

## 5. Repository Structure

```
Airlytics/
├── .github/
│   └── workflows/
│       ├── ci.yml            # CI: pytest (Beam transforms), terraform validate/fmt, Dataform compile
│       └── cd.yml            # CD: WIF OIDC -> terraform apply dev -> deploy dev -> approval gate -> prod
├── dashboard/                # Real-Time Airspace & Route Analytics Dashboard
│   ├── app.py                # Streamlit UI with Live Map, Sessionization, & Congestion views
│   ├── data_provider.py      # Telemetry provider (OpenSky live feed, BigQuery, or synthetic)
│   └── requirements.txt      # streamlit, plotly, pydeck, pandas
├── dataflow/                 # Apache Beam streaming pipeline & Flex Template
│   ├── Dockerfile            # Container definition for Dataflow Flex Template
│   ├── metadata.json         # Flex Template CLI/UI parameters definition
│   ├── pipeline.py           # Core streaming pipeline (Pub/Sub -> Beam -> BigQuery)
│   ├── requirements.txt      # apache-beam[gcp], google-cloud-bigquery, etc.
│   ├── schemas.py            # BigQuery schemas for streaming sinks
│   ├── transforms/           # Modular Beam DoFn transforms (sessionization, DLQ, late data)
│   └── tests/                # Unit tests with synthetic flight trajectory fixtures
├── dataform/                 # Dataform project definitions
│   ├── definitions/          # SQLX models (dim_airport, dim_aircraft, fact_flight_leg, etc.)
│   │   ├── assertions/       # Automated quality assertions (deduplication, freshness, duration)
│   │   └── sources.js        # Declarations of raw BigQuery tables
│   ├── package.json          # @dataform/core dependencies
│   └── workflow_settings.yaml# Project and dataset settings
├── ingest/                   # OpenSky REST API Poller service
│   ├── Dockerfile            # Multi-stage lightweight Python 3.11 container
│   ├── config.py             # Environment configurations (bounding box, intervals)
│   ├── main.py               # Cloud Run Job entrypoint with signal handlers
│   ├── models.py             # OpenSky ADS-B vector decoding and validation
│   ├── poller.py             # REST client with exponential backoff & 429 resilience
│   ├── publisher.py          # Pub/Sub batch publisher with deadletter routing
│   ├── requirements.txt      # requests, google-cloud-pubsub
│   └── tests/                # Unit tests with mocked HTTP responses
├── infra/                    # Multi-environment Terraform Infrastructure as Code
│   ├── dev/                  # Development workspace (backend, variables, tfvars.example)
│   ├── prod/                 # Production workspace (backend, variables, tfvars.example)
│   └── modules/              # Reusable Terraform modules
│       ├── bigquery/         # Datasets, partitioned/clustered tables, lifecycle policies
│       ├── budget/           # Google Cloud Billing Budget alerts (50%, 80%, 100%)
│       ├── cloud_run/        # Ingestion Cloud Run Job + Cloud Scheduler cron
│       ├── dataflow/         # Storage buckets (temp/staging), worker IAM roles
│       ├── monitoring/       # 10-minute freshness alert policy + email notification
│       └── pubsub/           # Raw pings topic, subscription, and dead-letter topic
├── reference/                # Static reference datasets (OurAirports hub airports CSV)
├── scripts/                  # Helper utilities (seeding reference airports data)
├── sql/                      # Standalone DDL files (raw_tables.sql, curated_tables.sql)
├── tests/                    # Project-level verification tests (HCL, schemas, workflows)
├── DECISIONS.md              # Comprehensive Architectural Decision Records (ADR 000-006)
└── README.md                 # Project documentation
```

---

## 6. Local Setup & Testing Instructions

### 6.1 Prerequisites
- Python 3.11+
- Git

### 6.2 Running the Unit Test Suite
Clone the repository and execute the full test suite (covering Ingestion, Apache Beam transforms, Dataform structure, BigQuery DDL, and CI/CD security compliance):

```bash
# Clone the repository
git clone https://github.com/mivishvajeet/Airlytics.git
cd Airlytics

# Execute all unit tests
PYTHONPATH=. python3 -m unittest discover -s ingest/tests -p "test_*.py"
PYTHONPATH=. python3 -m unittest discover -s dataflow/tests -p "test_*.py"
PYTHONPATH=. python3 -m unittest discover -s tests -p "test_*.py"
```

### 6.3 Running Ingestion Locally in Dry-Run Mode
You can test the OpenSky poller and payload parsing locally without publishing to GCP:

```bash
export DRY_RUN=true
export POLL_INTERVAL_SECONDS=5.0
export RUN_DURATION_SECONDS=15
export BBOX_LAMIN=48.0
export BBOX_LOMIN=2.0
export BBOX_LAMAX=54.0
export BBOX_LOMAX=8.0

python3 -m ingest.main
```

### 6.4 Launching the Interactive Analytics Dashboard
Launch the real-time Airlytics Streamlit dashboard to explore live airspace maps, sessionized flight legs, and airport congestion metrics:

```bash
pip install -r dashboard/requirements.txt
streamlit run dashboard/app.py
```
*(Supports toggling between Live OpenSky streaming and Synthetic corridor simulation)*

---

## 7. CI/CD & Production Deployment

All deployments are driven by GitHub Actions using Workload Identity Federation (WIF). **No service account keys are stored or permitted.**

### 7.1 Pull Request Pipeline (`ci.yml`)
On every PR targeting `main`:
1. Executes `flake8` linting and `pytest` for all Apache Beam transforms and ingestion parsers.
2. Validates Terraform format (`terraform fmt -check`) and validates both `dev` and `prod` modules (`terraform validate`).
3. Compiles the Dataform DAG using `@dataform/cli` to verify SQLX model syntax and dependency references.

### 7.2 Continuous Deployment Pipeline (`cd.yml`)
On merge to `main`:
1. Authenticates to Google Cloud via GitHub Actions OIDC token exchange.
2. Runs `terraform apply` for `infra/dev`.
3. Builds and pushes the OpenSky Ingestion container to Artifact Registry.
4. Builds the Dataflow Flex Template and launches/updates the streaming job in `dev`.
5. Pauses at the **`production` GitHub Environment approval gate** for review.
6. Upon manual approval, executes the production rollout for `infra/prod`.

---

## 8. Architectural Decision Log

Every design choice, trade-off, and rejected alternative is documented in [DECISIONS.md](DECISIONS.md):
- **ADR 000**: Architecture Baseline & Technology Selection
- **ADR 001**: OpenSky Polling & Rate-Limit Resilient Ingestion
- **ADR 002**: BigQuery Storage Strategy, Partitioning, and Clustering
- **ADR 003**: Apache Beam Windowing, Sessionization, and Late Data Strategy
- **ADR 004**: Dataform Incremental ELT, GIS Geofencing, and Quality Assertions
- **ADR 005**: Multi-Environment Infrastructure as Code & Cost Guardrails
- **ADR 006**: Zero-Key CI/CD via Workload Identity Federation (WIF) and Approval Gates

