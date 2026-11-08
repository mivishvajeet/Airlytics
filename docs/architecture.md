# Airlytics Architecture & Data Specifications

This document outlines the detailed technical specifications of the streaming and analytical data pipelines within Airlytics.

---

## 1. End-to-End Data Pipeline Architecture

```
[OpenSky REST API]
       │
       ▼ (10-15s polling, 429 backoff)
[Cloud Run Job] ──(malformed)──► [Pub/Sub DLQ]
       │
       ▼ (normalized JSON)
[Cloud Pub/Sub: raw-flight-pings]
       │
       ▼ (streaming pull)
[Apache Beam on Cloud Dataflow]
  ├── Session Windowing (15m gap) ──► [fact_flight_leg candidates]
  ├── Late Data Filter (>10m)    ──► [raw_flights.late_pings]
  └── Raw Telemetry Ingestion     ──► [raw_flights.raw_pings]
                                           │
                                           ▼ (incremental ELT)
                                  [BigQuery GIS & Dataform]
                                    ├── dim_airport (5km geofences)
                                    ├── fact_flight_leg
                                    ├── fact_airport_event
                                    └── fact_route_agg
```

---

## 2. Telemetry Schemas

### 2.1 Raw Telemetry (`raw_flights.raw_pings`)
- Partitioning: `DATE(event_timestamp)`
- Clustering: `icao24`, `callsign`
- Expiration: 90 days

| Field | Type | Mode | Description |
| :--- | :--- | :--- | :--- |
| `message_id` | STRING | REQUIRED | Deterministic surrogate key (`MD5(icao24 \|\| timestamp)`) |
| `ingest_timestamp` | TIMESTAMP | REQUIRED | Ingestion time by Cloud Run |
| `event_timestamp` | TIMESTAMP | REQUIRED | Transponder broadcast time |
| `event_time_epoch` | INTEGER | REQUIRED | Epoch timestamp in seconds |
| `icao24` | STRING | REQUIRED | Aircraft 24-bit transponder address |
| `callsign` | STRING | NULLABLE | Commercial flight callsign (e.g. BAW123) |
| `origin_country` | STRING | NULLABLE | Country of aircraft registry |
| `longitude` | FLOAT | REQUIRED | GPS Longitude (-180 to 180) |
| `latitude` | FLOAT | REQUIRED | GPS Latitude (-90 to 90) |
| `baro_altitude` | FLOAT | NULLABLE | Barometric altitude in meters |
| `on_ground` | BOOLEAN | REQUIRED | Transponder ground status |
| `velocity` | FLOAT | NULLABLE | Ground speed in meters/second |
| `true_track` | FLOAT | NULLABLE | Heading angle in degrees (0-360) |
| `vertical_rate` | FLOAT | NULLABLE | Climb/descent rate in meters/second |

---

## 3. BigQuery GIS Spatial Processing

Airports are modeled with pre-calculated 5,000-meter geodesic polygons:
```sql
ST_BUFFER(ST_GEOGPOINT(longitude_deg, latitude_deg), 5000.0) AS geofence_polygon
```

Takeoff and landing events are detected using spatial containment and aircraft kinematics:
```sql
-- Takeoff: inside geofence climbing at >= 1.5 m/s
ST_CONTAINS(a.geofence_polygon, p.ping_point) AND p.vertical_rate >= 1.5

-- Landing: inside geofence descending at <= -1.5 m/s
ST_CONTAINS(a.geofence_polygon, p.ping_point) AND p.vertical_rate <= -1.5
```

