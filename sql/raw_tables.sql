-- =============================================================================
-- Airlytics: BigQuery Raw Storage Layer DDL
-- Dataset: raw_flights
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS `raw_flights`
OPTIONS (
  location = 'EU',
  description = 'Raw streaming ingestion and dead-letter datasets for ADS-B flight pings'
);

-- 1. Raw Flight Pings Table
-- Direct append-only streaming sink from Dataflow.
-- Partitioned daily by event_timestamp with a 90-day partition expiration for cost guardrails.
-- Clustered by icao24 and callsign for high-efficiency downstream incremental scans.
CREATE TABLE IF NOT EXISTS `raw_flights.raw_pings` (
  message_id STRING NOT NULL OPTIONS(description="Unique UUIDv4 per message"),
  ingest_timestamp TIMESTAMP NOT NULL OPTIONS(description="UTC timestamp when ingested by poller"),
  event_timestamp TIMESTAMP NOT NULL OPTIONS(description="UTC timestamp of the ADS-B transponder position report"),
  event_time_epoch INT64 NOT NULL OPTIONS(description="Unix epoch seconds of position report"),
  icao24 STRING NOT NULL OPTIONS(description="24-bit ICAO aircraft address in hexadecimal"),
  callsign STRING OPTIONS(description="Eight-character callsign broadcast by aircraft transponder"),
  origin_country STRING OPTIONS(description="Country inferred from the 24-bit ICAO address allocation"),
  longitude FLOAT64 NOT NULL OPTIONS(description="WGS-84 longitude in decimal degrees"),
  latitude FLOAT64 NOT NULL OPTIONS(description="WGS-84 latitude in decimal degrees"),
  baro_altitude FLOAT64 OPTIONS(description="Barometric altitude in meters"),
  on_ground BOOLEAN NOT NULL OPTIONS(description="Flag indicating if aircraft is on ground"),
  velocity FLOAT64 OPTIONS(description="Ground speed in meters per second"),
  true_track FLOAT64 OPTIONS(description="True track in decimal degrees clockwise from geographic north"),
  vertical_rate FLOAT64 OPTIONS(description="Vertical speed in meters per second"),
  geo_altitude FLOAT64 OPTIONS(description="Geometric altitude in meters"),
  squawk STRING OPTIONS(description="Transponder 4-digit octal squawk code"),
  spi BOOLEAN OPTIONS(description="Special Purpose Indicator transponder flag"),
  position_source INT64 OPTIONS(description="0=ADS-B, 1=ASTERIX, 2=MLAT, 3=FLARM")
)
PARTITION BY DATE(event_timestamp)
CLUSTER BY icao24, callsign
OPTIONS (
  partition_expiration_days = 90.0,
  description = "Append-only raw flight telemetry stream partitioned by day and clustered by aircraft"
);

-- 2. Late Pings Table
-- Captures events arriving past Apache Beam allowed lateness window for auditing lag.
CREATE TABLE IF NOT EXISTS `raw_flights.late_pings` (
  message_id STRING NOT NULL,
  ingest_timestamp TIMESTAMP NOT NULL,
  event_timestamp TIMESTAMP NOT NULL,
  event_time_epoch INT64 NOT NULL,
  icao24 STRING NOT NULL,
  callsign STRING,
  longitude FLOAT64,
  latitude FLOAT64,
  baro_altitude FLOAT64,
  on_ground BOOLEAN,
  velocity FLOAT64,
  true_track FLOAT64,
  vertical_rate FLOAT64,
  lateness_seconds FLOAT64 OPTIONS(description="Difference between watermark and event timestamp")
)
PARTITION BY DATE(event_timestamp)
CLUSTER BY icao24
OPTIONS (
  partition_expiration_days = 30.0,
  description = "Events arriving after streaming watermark allowed lateness cutoff"
);

-- 3. Dead-Letter Pings Table
-- Captures corrupt, unparseable, or out-of-bounds telemetry payloads.
CREATE TABLE IF NOT EXISTS `raw_flights.deadletter_pings` (
  deadletter_id STRING NOT NULL,
  received_timestamp TIMESTAMP NOT NULL,
  reason STRING NOT NULL,
  raw_payload STRING,
  source_topic STRING
)
PARTITION BY DATE(received_timestamp)
OPTIONS (
  partition_expiration_days = 30.0,
  description = "Malformed payloads or deserialization errors routed away from the main streaming pipeline"
);

