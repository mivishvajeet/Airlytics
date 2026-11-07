variable "project_id" {
  type        = string
  description = "GCP Project ID"
}

variable "environment" {
  type        = string
  description = "Deployment environment (dev or prod)"
}

variable "alert_email" {
  type        = string
  default     = "alerts@example.com"
  description = "Target email address for operational alerts"
}

variable "pubsub_topic_id" {
  type        = string
  description = "Pub/Sub topic ID to monitor for message ingestion"
}

