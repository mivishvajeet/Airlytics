# Notification channel for email alerts
resource "google_monitoring_notification_channel" "email" {
  display_name = "Airlytics Ops Alerts (${var.environment})"
  type         = "email"
  project      = var.project_id

  labels = {
    email_address = var.alert_email
  }
}

# Freshness Alert: Alerts if no telemetry messages land for 10 minutes
resource "google_monitoring_alert_policy" "freshness_alert" {
  display_name = "Airlytics Telemetry Freshness Alert - No Data for 10m (${var.environment})"
  project      = var.project_id
  combiner     = "OR"

  conditions {
    display_name = "Absence of incoming flight pings on Pub/Sub topic"

    condition_absent {
      filter   = "metric.type=\"pubsub.googleapis.com/topic/send_message_operation_count\" AND resource.type=\"pubsub_topic\" AND resource.labels.topic_id=\"${var.pubsub_topic_id}\""
      duration = "600s" # 10 minutes threshold

      aggregations {
        alignment_period     = "60s"
        per_series_aligner   = "ALIGN_RATE"
        cross_series_reducer = "REDUCE_SUM"
      }
    }
  }

  notification_channels = [google_monitoring_notification_channel.email.name]

  documentation {
    content   = "CRITICAL: No flight telemetry has been published to Pub/Sub topic ${var.pubsub_topic_id} for over 10 minutes. Check OpenSky poller Cloud Run job logs and API connectivity."
    mime_type = "text/markdown"
  }

  user_labels = {
    severity    = "critical"
    environment = var.environment
  }
}

