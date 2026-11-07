variable "project_id" {
  type        = string
  default     = "airlytics-prod-123456" # Parameterized fake ID for prod
  description = "GCP Project ID for production"
}

variable "environment" {
  type        = string
  default     = "prod"
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
  default     = 50
  description = "Monthly budget limit in USD"
}

variable "alert_email" {
  type        = string
  default     = "prod-alerts@example.com"
  description = "Alert email recipient"
}

variable "ingestion_image" {
  type        = string
  default     = "europe-west1-docker.pkg.dev/airlytics-prod-123456/airlytics/ingest:latest"
  description = "Container image for OpenSky poller"
}
