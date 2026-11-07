"""BigQuery table schemas and helper dictionaries for Apache Beam WriteToBigQuery."""

from typing import Any, Dict

RAW_PINGS_SCHEMA: Dict[str, Any] = {
    "fields": [
        {"name": "message_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "ingest_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "event_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "event_time_epoch", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "icao24", "type": "STRING", "mode": "REQUIRED"},
        {"name": "callsign", "type": "STRING", "mode": "NULLABLE"},
        {"name": "origin_country", "type": "STRING", "mode": "NULLABLE"},
        {"name": "longitude", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "latitude", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "baro_altitude", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "on_ground", "type": "BOOLEAN", "mode": "REQUIRED"},
        {"name": "velocity", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "true_track", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "vertical_rate", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "geo_altitude", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "squawk", "type": "STRING", "mode": "NULLABLE"},
        {"name": "spi", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "position_source", "type": "INTEGER", "mode": "NULLABLE"},
    ]
}

LATE_PINGS_SCHEMA: Dict[str, Any] = {
    "fields": [
        {"name": "message_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "ingest_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "event_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "event_time_epoch", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "icao24", "type": "STRING", "mode": "REQUIRED"},
        {"name": "callsign", "type": "STRING", "mode": "NULLABLE"},
        {"name": "longitude", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "latitude", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "baro_altitude", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "on_ground", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "velocity", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "true_track", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "vertical_rate", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "lateness_seconds", "type": "FLOAT", "mode": "NULLABLE"},
    ]
}

DEADLETTER_PINGS_SCHEMA: Dict[str, Any] = {
    "fields": [
        {"name": "deadletter_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "received_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "reason", "type": "STRING", "mode": "REQUIRED"},
        {"name": "raw_payload", "type": "STRING", "mode": "NULLABLE"},
        {"name": "source_topic", "type": "STRING", "mode": "NULLABLE"},
    ]
}

FLIGHT_LEGS_SCHEMA: Dict[str, Any] = {
    "fields": [
        {"name": "flight_leg_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "icao24", "type": "STRING", "mode": "REQUIRED"},
        {"name": "callsign", "type": "STRING", "mode": "NULLABLE"},
        {"name": "start_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "end_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "duration_minutes", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "origin_lat", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "origin_lon", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "dest_lat", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "dest_lon", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "max_altitude_m", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "max_velocity_mps", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "avg_velocity_mps", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "total_pings", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "session_gap_minutes", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}
