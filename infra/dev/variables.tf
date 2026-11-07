variable "project_id" {
  type        = string
  default     = "airlytics-dev-123456" # Parameterized fake ID for dev
  description = "GCP Project ID for development"
}

variable "environment" {
  type        = string
  default     = "dev"
  description = "Environment identifier"
}

variable "region" {
  type        = string
  default     = "europe-west1"
  description = "Primary GCP deployment region"
}

variable "bq_location" {
  type        = string
  default     = "EU"
  description = "BigQuery dataset location"
}

variable "billing_account_id" {
  type        = string
  default     = "000000-000000-000000"
  description = "GCP Billing Account ID"
}

variable "monthly_budget_amount" {
  type        = number
  default     = 25
  description = "Monthly budget limit in USD"
}

variable "alert_email" {
  type        = string
  default     = "dev-alerts@example.com"
  description = "Alert email recipient"
}

variable "ingestion_image" {
  type        = string
  default     = "europe-west1-docker.pkg.dev/airlytics-dev-123456/airlytics/ingest:latest"
  description = "Container image for OpenSky poller"
}
