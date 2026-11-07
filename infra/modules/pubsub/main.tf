# Dead-Letter Topic
resource "google_pubsub_topic" "deadletter" {
  name    = "${var.dlq_topic_name}-${var.environment}"
  project = var.project_id

  labels = {
    environment = var.environment
    managed_by  = "terraform"
    service     = "airlytics"
  }
}

# Primary Flight Pings Ingestion Topic
resource "google_pubsub_topic" "flight_pings" {
  name    = "${var.topic_name}-${var.environment}"
  project = var.project_id

  message_retention_duration = "86400s" # 24 hours to limit storage costs

  labels = {
    environment = var.environment
    managed_by  = "terraform"
    service     = "airlytics"
  }
}

# Dataflow Ingestion Subscription with Dead-Letter Policy
resource "google_pubsub_subscription" "dataflow_sub" {
  name    = "dataflow-${var.topic_name}-${var.environment}-sub"
  project = var.project_id
  topic   = google_pubsub_topic.flight_pings.id

  ack_deadline_seconds       = var.ack_deadline_seconds
  retain_acked_messages      = false
  message_retention_duration = "86400s" # 1 day retention

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.deadletter.id
    max_delivery_attempts = 5
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  labels = {
    environment = var.environment
    managed_by  = "terraform"
    service     = "airlytics"
  }
}

