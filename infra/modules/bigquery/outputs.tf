output "raw_dataset_id" {
  value       = google_bigquery_dataset.raw_flights.dataset_id
  description = "Dataset ID of the raw flights dataset"
}

output "curated_dataset_id" {
  value       = google_bigquery_dataset.curated_flights.dataset_id
  description = "Dataset ID of the curated analytics dataset"
}

output "raw_pings_table_id" {
  value       = "${var.project_id}:${google_bigquery_dataset.raw_flights.dataset_id}.${google_bigquery_table.raw_pings.table_id}"
  description = "Fully qualified table ID of raw_pings"
}

output "late_pings_table_id" {
  value       = "${var.project_id}:${google_bigquery_dataset.raw_flights.dataset_id}.${google_bigquery_table.late_pings.table_id}"
  description = "Fully qualified table ID of late_pings"
}

output "deadletter_table_id" {
  value       = "${var.project_id}:${google_bigquery_dataset.raw_flights.dataset_id}.${google_bigquery_table.deadletter_pings.table_id}"
  description = "Fully qualified table ID of deadletter_pings"
}

