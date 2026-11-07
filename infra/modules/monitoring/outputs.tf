output "freshness_alert_id" {
  value       = google_monitoring_alert_policy.freshness_alert.id
  description = "Resource ID of the freshness alert policy"
}

