output "job_name" {
  value       = google_cloud_run_v2_job.poller_job.name
  description = "Name of the Cloud Run Ingestion Job"
}

output "job_id" {
  value       = google_cloud_run_v2_job.poller_job.id
  description = "Resource ID of the Cloud Run Ingestion Job"
}

output "service_account_email" {
  value       = google_service_account.ingest_sa.email
  description = "Email of the Ingestion Job service account"
}
