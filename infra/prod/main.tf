terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }

  backend "gcs" {
    bucket = "airlytics-tfstate-prod"
    prefix = "terraform/state"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# 1. Pub/Sub Module
module "pubsub" {
  source         = "../modules/pubsub"
  project_id     = var.project_id
  environment    = var.environment
  topic_name     = "raw-flight-pings"
  dlq_topic_name = "deadletter-pings"
}

# 2. BigQuery Module
module "bigquery" {
  source                 = "../modules/bigquery"
  project_id             = var.project_id
  environment            = var.environment
  location               = var.bq_location
  raw_retention_days     = 90
  curated_retention_days = 365
}

# 3. Cloud Run Ingestion Module
module "cloud_run" {
  source                   = "../modules/cloud_run"
  project_id               = var.project_id
  environment              = var.environment
  region                   = var.region
  ingestion_image          = var.ingestion_image
  pubsub_topic_name        = module.pubsub.topic_name
  deadletter_topic_name    = "deadletter-pings-${var.environment}"
  schedule_cron            = "*/10 * * * *" # Continuous production schedule (every 10m)
  job_run_duration_seconds = 540
}

# 4. Dataflow Resources Module
module "dataflow" {
  source      = "../modules/dataflow"
  project_id  = var.project_id
  environment = var.environment
  region      = var.region
}

# 5. Monitoring & Freshness Alert Module
module "monitoring" {
  source          = "../modules/monitoring"
  project_id      = var.project_id
  environment     = var.environment
  alert_email     = var.alert_email
  pubsub_topic_id = module.pubsub.topic_name
}

# 6. Budget Guardrail Module
module "budget" {
  source                = "../modules/budget"
  project_id            = var.project_id
  environment           = var.environment
  billing_account_id    = var.billing_account_id
  monthly_budget_amount = var.monthly_budget_amount
}
