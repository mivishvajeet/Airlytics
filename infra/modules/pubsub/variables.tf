variable "project_id" {
  type        = string
  description = "GCP Project ID (parameterized, e.g. airlytics-dev-123456)"
}

variable "environment" {
  type        = string
  description = "Deployment environment (dev or prod)"
}

variable "topic_name" {
  type        = string
  default     = "raw-flight-pings"
  description = "Primary Pub/Sub topic name for raw flight pings"
}

variable "dlq_topic_name" {
  type        = string
  default     = "deadletter-pings"
  description = "Dead letter topic name for unparseable payloads"
}

variable "ack_deadline_seconds" {
  type        = number
  default     = 60
  description = "Pub/Sub subscription acknowledgment deadline"
}

