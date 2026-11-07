variable "project_id" {
  type        = string
  description = "GCP Project ID"
}

variable "environment" {
  type        = string
  description = "Deployment environment (dev or prod)"
}

variable "location" {
  type        = string
  default     = "EU"
  description = "BigQuery dataset geographical location"
}

variable "raw_retention_days" {
  type        = number
  default     = 90
  description = "Partition expiration in days for raw telemetry"
}

variable "curated_retention_days" {
  type        = number
  default     = 365
  description = "Partition expiration in days for curated fact tables"
}
