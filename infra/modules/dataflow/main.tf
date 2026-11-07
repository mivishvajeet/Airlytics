# Service account for Dataflow workers
resource "google_service_account" "dataflow_sa" {
  account_id   = "airlytics-dataflow-sa-${var.environment}"
  display_name = "Airlytics Dataflow Worker Service Account (${var.environment})"
  project      = var.project_id
}

# IAM: Dataflow Worker permissions
resource "google_project_iam_member" "dataflow_worker" {
  project = var.project_id
  role    = "roles/dataflow.worker"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

# IAM: BigQuery Data Editor permissions
resource "google_project_iam_member" "dataflow_bq_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

# IAM: Pub/Sub Subscriber permissions
resource "google_project_iam_member" "dataflow_pubsub_sub" {
  project = var.project_id
  role    = "roles/pubsub.subscriber"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

# GCS Bucket for Dataflow staging, temp, and flex template metadata
resource "google_storage_bucket" "dataflow_bucket" {
  name                        = "airlytics-dataflow-${var.environment}-${var.project_id}"
  project                     = var.project_id
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true

  # Cost constraint: Automatically purge temp objects after 7 days
  lifecycle_rule {
    condition {
      age = 7
    }
    action {
      type = "Delete"
    }
  }

  labels = {
    environment = var.environment
    managed_by  = "terraform"
    service     = "dataflow"
  }
}

# GCS Bucket for Reference datasets (OurAirports open data)
resource "google_storage_bucket" "reference_bucket" {
  name                        = "airlytics-reference-${var.environment}-${var.project_id}"
  project                     = var.project_id
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = false

  labels = {
    environment = var.environment
    managed_by  = "terraform"
    service     = "reference"
  }
}

# IAM: Dataflow access to GCS buckets
resource "google_storage_bucket_iam_member" "dataflow_bucket_admin" {
  bucket = google_storage_bucket.dataflow_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

resource "google_storage_bucket_iam_member" "reference_bucket_viewer" {
  bucket = google_storage_bucket.reference_bucket.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

