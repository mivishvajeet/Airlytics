# 1. Raw Telemetry Dataset
resource "google_bigquery_dataset" "raw_flights" {
  dataset_id  = "raw_flights_${var.environment}"
  project     = var.project_id
  location    = var.location
  description = "Append-only raw flight telemetry landed by Apache Beam"

  labels = {
    environment = var.environment
    layer       = "raw"
    managed_by  = "terraform"
  }
}

# 2. Curated Analytics Dataset
resource "google_bigquery_dataset" "curated_flights" {
  dataset_id  = "curated_flights_${var.environment}"
  project     = var.project_id
  location    = var.location
  description = "Curated flight legs, airport events, route aggregates, and dimensions"

  labels = {
    environment = var.environment
    layer       = "curated"
    managed_by  = "terraform"
  }
}

# 3. Raw Pings Table
resource "google_bigquery_table" "raw_pings" {
  dataset_id = google_bigquery_dataset.raw_flights.dataset_id
  table_id   = "raw_pings"
  project    = var.project_id

  time_partitioning {
    type                     = "DAY"
    field                    = "event_timestamp"
    expiration_ms            = var.raw_retention_days * 86400000
    require_partition_filter = false
  }

  clustering = ["icao24", "callsign"]

  schema = jsonencode([
    { name = "message_id", type = "STRING", mode = "REQUIRED" },
    { name = "ingest_timestamp", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "event_timestamp", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "event_time_epoch", type = "INTEGER", mode = "REQUIRED" },
    { name = "icao24", type = "STRING", mode = "REQUIRED" },
    { name = "callsign", type = "STRING", mode = "NULLABLE" },
    { name = "origin_country", type = "STRING", mode = "NULLABLE" },
    { name = "longitude", type = "FLOAT", mode = "REQUIRED" },
    { name = "latitude", type = "FLOAT", mode = "REQUIRED" },
    { name = "baro_altitude", type = "FLOAT", mode = "NULLABLE" },
    { name = "on_ground", type = "BOOLEAN", mode = "REQUIRED" },
    { name = "velocity", type = "FLOAT", mode = "NULLABLE" },
    { name = "true_track", type = "FLOAT", mode = "NULLABLE" },
    { name = "vertical_rate", type = "FLOAT", mode = "NULLABLE" },
    { name = "geo_altitude", type = "FLOAT", mode = "NULLABLE" },
    { name = "squawk", type = "STRING", mode = "NULLABLE" },
    { name = "spi", type = "BOOLEAN", mode = "NULLABLE" },
    { name = "position_source", type = "INTEGER", mode = "NULLABLE" }
  ])

  labels = {
    environment = var.environment
    managed_by  = "terraform"
  }
}

# 4. Late Pings Table
resource "google_bigquery_table" "late_pings" {
  dataset_id = google_bigquery_dataset.raw_flights.dataset_id
  table_id   = "late_pings"
  project    = var.project_id

  time_partitioning {
    type          = "DAY"
    field         = "event_timestamp"
    expiration_ms = 30 * 86400000
  }

  clustering = ["icao24"]

  schema = jsonencode([
    { name = "message_id", type = "STRING", mode = "REQUIRED" },
    { name = "ingest_timestamp", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "event_timestamp", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "event_time_epoch", type = "INTEGER", mode = "REQUIRED" },
    { name = "icao24", type = "STRING", mode = "REQUIRED" },
    { name = "callsign", type = "STRING", mode = "NULLABLE" },
    { name = "longitude", type = "FLOAT", mode = "NULLABLE" },
    { name = "latitude", type = "FLOAT", mode = "NULLABLE" },
    { name = "baro_altitude", type = "FLOAT", mode = "NULLABLE" },
    { name = "on_ground", type = "BOOLEAN", mode = "NULLABLE" },
    { name = "velocity", type = "FLOAT", mode = "NULLABLE" },
    { name = "true_track", type = "FLOAT", mode = "NULLABLE" },
    { name = "vertical_rate", type = "FLOAT", mode = "NULLABLE" },
    { name = "lateness_seconds", type = "FLOAT", mode = "NULLABLE" }
  ])
}

# 5. Dead-Letter Pings Table
resource "google_bigquery_table" "deadletter_pings" {
  dataset_id = google_bigquery_dataset.raw_flights.dataset_id
  table_id   = "deadletter_pings"
  project    = var.project_id

  time_partitioning {
    type          = "DAY"
    field         = "received_timestamp"
    expiration_ms = 30 * 86400000
  }

  schema = jsonencode([
    { name = "deadletter_id", type = "STRING", mode = "REQUIRED" },
    { name = "received_timestamp", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "reason", type = "STRING", mode = "REQUIRED" },
    { name = "raw_payload", type = "STRING", mode = "NULLABLE" },
    { name = "source_topic", type = "STRING", mode = "NULLABLE" }
  ])
}

