# Service account for Cloud Run Ingestion Job
resource "google_service_account" "ingest_sa" {
  account_id   = "airlytics-ingest-sa-${var.environment}"
  display_name = "Airlytics OpenSky Ingestion Service Account (${var.environment})"
  project      = var.project_id
}

# Artifact Registry Repository for Airlytics container images
resource "google_artifact_registry_repository" "airlytics_repo" {
  project       = var.project_id
  location      = var.region
  repository_id = "airlytics"
  description   = "Docker repository for Airlytics container images"
  format        = "DOCKER"
}

# Grant Pub/Sub Publisher role to Ingestion Service Account
resource "google_project_iam_member" "pubsub_publisher" {
  project = var.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.ingest_sa.email}"
}

# Grant Cloud Run Invoker role to Cloud Scheduler service agent
resource "google_project_iam_member" "run_invoker" {
  project = var.project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${google_service_account.ingest_sa.email}"
}

# Cloud Run Job: OpenSky Poller
resource "google_cloud_run_v2_job" "poller_job" {
  name     = "airlytics-poller-${var.environment}"
  location = var.region
  project  = var.project_id

  template {
    task_count = 1

    template {
      max_retries     = 3
      timeout         = "${var.job_run_duration_seconds + 60}s"
      service_account = google_service_account.ingest_sa.email

      containers {
        image = var.ingestion_image

        resources {
          limits = {
            cpu    = "1000m"
            memory = "512Mi" # Lightweight resource sizing within free tier
          }
        }

        env {
          name  = "GCP_PROJECT_ID"
          value = var.project_id
        }
        env {
          name  = "PUBSUB_TOPIC_ID"
          value = var.pubsub_topic_name
        }
        env {
          name  = "DEADLETTER_TOPIC_ID"
          value = var.deadletter_topic_name
        }
        env {
          name  = "POLL_INTERVAL_SECONDS"
          value = "10.0"
        }
        env {
          name  = "RUN_DURATION_SECONDS"
          value = tostring(var.job_run_duration_seconds)
        }
      }
    }
  }

  labels = {
    environment = var.environment
    managed_by  = "terraform"
    service     = "ingest"
  }
}

# Cloud Scheduler: Trigger Ingestion Job during scheduled operating hours
resource "google_cloud_scheduler_job" "poller_schedule" {
  name        = "airlytics-poller-schedule-${var.environment}"
  description = "Triggers the OpenSky ingestion Cloud Run job periodically during active hours"
  schedule    = var.schedule_cron
  time_zone   = "UTC"
  project     = var.project_id
  region      = var.region

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/v2/projects/${var.project_id}/locations/${var.region}/jobs/${google_cloud_run_v2_job.poller_job.name}:run"

    oauth_token {
      service_account_email = google_service_account.ingest_sa.email
    }
  }
}

