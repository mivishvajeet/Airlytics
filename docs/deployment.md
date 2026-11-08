# Airlytics Deployment & Infrastructure Guide

This document contains detailed instructions for deploying Airlytics to Google Cloud Platform, configuring Workload Identity Federation (WIF), and managing multi-environment infrastructure.

---

## 1. Prerequisites

- Google Cloud Platform account with billing enabled.
- `gcloud` CLI (or Google Cloud Shell).
- `terraform` >= 1.5.0.
- Python 3.11+.

---

## 2. Infrastructure as Code (Terraform)

Infrastructure is managed using modular Terraform separated into environment directories:
- `infra/dev/`: Development workspace with cost caps and minimal sizing.
- `infra/prod/`: Production workspace with redundant workers and strict alerting.

### 2.1 Initializing and Provisioning Dev Environment

```bash
cd infra/dev

# 1. Initialize Terraform
terraform init

# 2. Plan the deployment
terraform plan -var="project_id=<YOUR_GCP_PROJECT_ID>"

# 3. Apply infrastructure
terraform apply -var="project_id=<YOUR_GCP_PROJECT_ID>"
```

### 2.2 Provisioned Cloud Resources

| Resource | Terraform Module | Purpose |
| :--- | :--- | :--- |
| **Pub/Sub Topics** | `infra/modules/pubsub` | High-throughput buffer (`raw-flight-pings`) + Dead-Letter Queue |
| **BigQuery Datasets** | `infra/modules/bigquery` | Raw partitioned tables & curated dimensional marts |
| **Cloud Run Job** | `infra/modules/cloud_run` | Scheduled OpenSky REST poller container |
| **Artifact Registry** | `infra/modules/cloud_run` | Private Docker repository for custom containers |
| **Dataflow Storage** | `infra/modules/dataflow` | GCS temp/staging buckets with 7-day TTL lifecycle |
| **Cloud Monitoring** | `infra/modules/monitoring` | 10-minute stream silence metric absence alert |
| **Cloud Billing Budget** | `infra/modules/budget` | Spending alerts at 50%, 80%, and 100% of threshold |

---

## 3. Data Seeding & Ingestion Execution

### 3.1 Seeding Airport Reference Geofences

```bash
pip install google-cloud-bigquery requests
python3 scripts/populate_live_data.py --project-id=<YOUR_GCP_PROJECT_ID>
```

### 3.2 Building and Deploying OpenSky Poller Container

```bash
# Build container image with Cloud Build
gcloud builds submit --tag europe-west1-docker.pkg.dev/<YOUR_GCP_PROJECT_ID>/airlytics/ingest:latest ingest/

# Update Cloud Run job
gcloud run jobs update airlytics-poller-dev \
  --image europe-west1-docker.pkg.dev/<YOUR_GCP_PROJECT_ID>/airlytics/ingest:latest \
  --region europe-west1

# Execute a one-time job run
gcloud run jobs execute airlytics-poller-dev --region europe-west1
```

---

## 4. CI/CD & Zero-Key Security via Workload Identity Federation

All automated deployments run via GitHub Actions using OpenID Connect (OIDC). **No persistent service account keys are stored or permitted.**

### 4.1 GitHub Actions Workflow Architecture

- **`ci.yml` (Pull Request)**:
  - Python linting (`flake8`) & unit tests (`pytest`).
  - Terraform validation (`fmt -check`, `validate`).
  - Dataform compilation verification (`@dataform/cli`).
- **`cd.yml` (Merge to `main`)**:
  - OIDC token exchange with Google Cloud Security Token Service.
  - Terraform apply for `dev`.
  - Artifact build and Dataflow template deployment.
  - Manual approval gate for `production` rollout.

### 4.2 GitHub Secrets & Variables Configuration

Configure these in your GitHub Repository under **Settings** → **Secrets and variables** → **Actions**:

| Name | Type | Description |
| :--- | :--- | :--- |
| `DEV_GCP_PROJECT_ID` | Variable | GCP Project ID for dev environment |
| `DEV_WIF_PROVIDER` | Secret | Workload Identity Pool provider resource name |
| `DEV_WIF_SA` | Secret | Service Account email with deployment permissions |
| `PROD_GCP_PROJECT_ID` | Variable | GCP Project ID for production |
| `PROD_WIF_PROVIDER` | Secret | Production Workload Identity Pool provider |
| `PROD_WIF_SA` | Secret | Production deployment Service Account email |

