output "pubsub_topic_id" {
  value       = module.pubsub.topic_id
  description = "Pub/Sub topic ID for raw flight pings"
}

output "pubsub_subscription_id" {
  value       = module.pubsub.subscription_id
  description = "Dataflow Pub/Sub subscription ID"
}

output "raw_pings_table_id" {
  value       = module.bigquery.raw_pings_table_id
  description = "BigQuery raw_pings table ID"
}

output "dataflow_sa_email" {
  value       = module.dataflow.service_account_email
  description = "Service account email for Dataflow execution"
}

output "dataflow_bucket_name" {
  value       = module.dataflow.dataflow_bucket_name
  description = "GCS bucket for Dataflow temp/staging"
}
