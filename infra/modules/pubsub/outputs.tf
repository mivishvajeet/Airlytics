output "topic_id" {
  value       = google_pubsub_topic.flight_pings.id
  description = "Full resource ID of the primary flight pings topic"
}

output "topic_name" {
  value       = google_pubsub_topic.flight_pings.name
  description = "Short name of the primary flight pings topic"
}

output "subscription_id" {
  value       = google_pubsub_subscription.dataflow_sub.id
  description = "Full resource ID of the Dataflow subscription"
}

output "deadletter_topic_id" {
  value       = google_pubsub_topic.deadletter.id
  description = "Full resource ID of the dead-letter topic"
}
