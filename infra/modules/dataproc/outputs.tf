output "dataproc_service_account_email" {
  value       = google_service_account.dataproc_sa.email
  description = "Email of the Dataproc Spark execution service account"
}

output "checkpoint_bucket_name" {
  value       = google_storage_bucket.spark_checkpoint_bucket.name
  description = "GCS bucket name for PySpark checkpoints and staging"
}

output "checkpoint_bucket_url" {
  value       = google_storage_bucket.spark_checkpoint_bucket.url
  description = "GCS bucket URL for PySpark checkpoints"
}

output "cluster_name" {
  value       = try(google_dataproc_cluster.ephemeral_cluster[0].name, "none")
  description = "Name of the managed Dataproc cluster if provisioned"
}
