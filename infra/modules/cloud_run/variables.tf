variable "project_id" {
  type        = string
  description = "GCP Project ID"
}

variable "environment" {
  type        = string
  description = "Deployment environment (dev or prod)"
}

variable "region" {
  type        = string
  default     = "europe-west1"
  description = "Cloud Run region"
}

variable "ingestion_image" {
  type        = string
  default     = "europe-west1-docker.pkg.dev/airlytics-dev-123456/airlytics/ingest:latest"
  description = "Container image URL for the OpenSky poller"
}

variable "pubsub_topic_name" {
  type        = string
  description = "Pub/Sub target topic name"
}

variable "deadletter_topic_name" {
  type        = string
  description = "Deadletter target topic name"
}

variable "schedule_cron" {
  type        = string
  default     = "*/15 8-22 * * *" # Runs every 15 min during active daylight hours
  description = "Cloud Scheduler cron expression for personal cost budget control"
}

variable "job_run_duration_seconds" {
  type        = number
  default     = 840 # 14 minutes per run, cleanly handing off before next cron trigger
  description = "Duration in seconds each Cloud Run job execution runs before terminating"
}

