output "service_account_email" {
  value       = google_service_account.dataflow_sa.email
  description = "Email of the Dataflow worker service account"
}

output "dataflow_bucket_name" {
  value       = google_storage_bucket.dataflow_bucket.name
  description = "GCS bucket for Dataflow temp and staging files"
}

output "reference_bucket_name" {
  value       = google_storage_bucket.reference_bucket.name
  description = "GCS bucket for airport reference data"
}

