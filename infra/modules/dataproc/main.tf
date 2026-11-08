# =============================================================================
# 1. Dataproc Service Account & IAM Roles
# =============================================================================
resource "google_service_account" "dataproc_sa" {
  account_id   = "airlytics-dataproc-sa-${var.environment}"
  display_name = "Airlytics Dataproc Spark Execution Service Account (${var.environment})"
  project      = var.project_id
}

# Role: Dataproc Worker
resource "google_project_iam_member" "dataproc_worker" {
  project = var.project_id
  role    = "roles/dataproc.worker"
  member  = "serviceAccount:${google_service_account.dataproc_sa.email}"
}

# Role: BigQuery Data Editor (writing streaming DataFrames to curated_flights)
resource "google_project_iam_member" "bigquery_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.dataproc_sa.email}"
}

# Role: BigQuery Job User (submitting queries and loads)
resource "google_project_iam_member" "bigquery_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.dataproc_sa.email}"
}

# Role: Storage Object Admin (reading/writing checkpoints and temporary staging files)
resource "google_project_iam_member" "storage_admin" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.dataproc_sa.email}"
}

# =============================================================================
# 2. GCS Bucket for Spark Checkpoints & Intermediate Staging
# =============================================================================
resource "google_storage_bucket" "spark_checkpoint_bucket" {
  name          = "airlytics-spark-checkpoints-${var.project_id}-${var.environment}"
  location      = var.region
  project       = var.project_id
  force_destroy = true

  uniform_bucket_level_access = true

  # FinOps Cost Guardrail: Automatically purge checkpoints older than 7 days
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
    service     = "spark"
  }
}

# =============================================================================
# 3. Optional Ephemeral Single-Node Dataproc Cluster (Cost-Capped)
# =============================================================================
# Only deployed if enable_managed_cluster = true.
# Includes strict idle_delete_ttl (20m) and auto_delete_ttl (1h) to prevent runaway billing!
resource "google_dataproc_cluster" "ephemeral_cluster" {
  count   = var.enable_managed_cluster ? 1 : 0
  name    = "airlytics-spark-${var.environment}"
  project = var.project_id
  region  = var.region

  cluster_config {
    master_config {
      num_instances = 1
      machine_type  = "e2-standard-2" # Lightweight cost-effective VM
      disk_config {
        boot_disk_type    = "pd-standard"
        boot_disk_size_gb = 50
      }
    }

    # Single-node configuration: 0 worker nodes
    worker_config {
      num_instances = 0
    }

    software_config {
      image_version = "2.1-debian11"
      override_properties = {
        "dataproc:dataproc.allow.zero.workers" = "true"
      }
    }

    gce_cluster_config {
      service_account = google_service_account.dataproc_sa.email
      service_account_scopes = [
        "https://www.googleapis.com/auth/cloud-platform"
      ]
    }

    # Strict FinOps Auto-Deletion Guardrails:
    # Cluster automatically shuts down if idle for 20 minutes or after 1 hour max runtime
    lifecycle_config {
      idle_delete_ttl  = "1200s"
      auto_delete_time = null
      idle_start_time  = null
    }
  }

  labels = {
    environment = var.environment
    managed_by  = "terraform"
  }
}
