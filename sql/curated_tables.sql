-- =============================================================================
-- Airlytics: BigQuery Curated Analytics Layer DDL
-- Dataset: curated_flights
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS `curated_flights`
OPTIONS (
  location = 'EU',
  description = 'Curated dimensional marts, sessionized legs, airport events, and route aggregations'
);

-- 1. Airport Dimension Table
-- Seeded from OurAirports CSV open data with BigQuery GIS Geography types.
CREATE TABLE IF NOT EXISTS `curated_flights.dim_airport` (
  airport_id INT64 NOT NULL OPTIONS(description="OurAirports internal numeric identifier"),
  ident STRING NOT NULL OPTIONS(description="ICAO 4-letter or local airport code (e.g. EHAM, EGLL, KJFK)"),
  type STRING OPTIONS(description="Airport classification: large_airport, medium_airport, small_airport, heliport"),
  name STRING NOT NULL OPTIONS(description="Official airport name"),
  latitude_deg FLOAT64 NOT NULL OPTIONS(description="Latitude in decimal degrees"),
  longitude_deg FLOAT64 NOT NULL OPTIONS(description="Longitude in decimal degrees"),
  elevation_ft INT64 OPTIONS(description="Airport elevation above sea level in feet"),
  continent STRING,
  iso_country STRING OPTIONS(description="ISO 3166-1 alpha-2 country code"),
  iso_region STRING OPTIONS(description="ISO region code"),
  municipality STRING OPTIONS(description="Primary city or municipality served"),
  scheduled_service STRING OPTIONS(description="yes/no flag for commercial passenger operations"),
  gps_code STRING,
  iata_code STRING OPTIONS(description="IATA 3-letter passenger code (e.g. AMS, LHR, JFK)"),
  local_code STRING,
  airport_point GEOGRAPHY NOT NULL OPTIONS(description="BigQuery GEOGRAPHY point for spatial calculations"),
  geofence_polygon GEOGRAPHY NOT NULL OPTIONS(description="5km geodesic buffer polygon for spatial containment joins")
)
CLUSTER BY ident, iata_code
OPTIONS (
  description = "Airports reference dimension with pre-calculated GIS geography point and buffer geofence"
);

-- 2. Aircraft Dimension Table (SCD-Capable)
CREATE TABLE IF NOT EXISTS `curated_flights.dim_aircraft` (
  icao24 STRING NOT NULL OPTIONS(description="24-bit ICAO hexadecimal airframe address"),
  latest_callsign STRING,
  origin_country STRING,
  first_seen_timestamp TIMESTAMP NOT NULL,
  last_seen_timestamp TIMESTAMP NOT NULL,
  total_pings_observed INT64 NOT NULL,
  is_active BOOLEAN NOT NULL
)
CLUSTER BY icao24
OPTIONS (
  description = "Aircraft registry derived from ADS-B transponder observations"
);

-- 3. Curated Flight Legs Fact Table (Sessionized)
CREATE TABLE IF NOT EXISTS `curated_flights.fact_flight_leg` (
  flight_leg_id STRING NOT NULL OPTIONS(description="Surrogate hash: MD5(icao24 || start_timestamp)"),
  icao24 STRING NOT NULL,
  callsign STRING,
  start_timestamp TIMESTAMP NOT NULL,
  end_timestamp TIMESTAMP NOT NULL,
  duration_minutes FLOAT64 NOT NULL,
  origin_airport_ident STRING OPTIONS(description="Ident of airport resolved at session start within 10km"),
  destination_airport_ident STRING OPTIONS(description="Ident of airport resolved at session end within 10km"),
  origin_iata STRING,
  destination_iata STRING,
  max_altitude_m FLOAT64,
  max_velocity_mps FLOAT64,
  avg_velocity_mps FLOAT64,
  total_pings INT64 NOT NULL,
  session_gap_minutes FLOAT64,
  created_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(start_timestamp)
CLUSTER BY origin_airport_ident, destination_airport_ident, icao24
OPTIONS (
  partition_expiration_days = 365.0,
  description = "Individual aircraft flight trajectories sessionized using gap-based windowing"
);

-- 4. Curated Airport Events Fact Table
CREATE TABLE IF NOT EXISTS `curated_flights.fact_airport_event` (
  event_id STRING NOT NULL OPTIONS(description="Surrogate hash: MD5(airport_ident || icao24 || event_timestamp)"),
  airport_ident STRING NOT NULL,
  airport_iata STRING,
  airport_name STRING,
  icao24 STRING NOT NULL,
  callsign STRING,
  event_type STRING NOT NULL OPTIONS(description="TAKEOFF or LANDING"),
  event_timestamp TIMESTAMP NOT NULL,
  altitude_m FLOAT64,
  velocity_mps FLOAT64,
  vertical_rate_mps FLOAT64,
  distance_to_airport_m FLOAT64,
  created_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(event_timestamp)
CLUSTER BY airport_ident, event_type
OPTIONS (
  partition_expiration_days = 365.0,
  description = "Takeoff and landing events resolved via spatial containment and vertical flight vector analysis"
);

-- 5. Curated Hourly Route Aggregations Fact Table
CREATE TABLE IF NOT EXISTS `curated_flights.fact_route_agg` (
  route_id STRING NOT NULL OPTIONS(description="Origin to destination pair: ORIGIN-DESTINATION"),
  origin_airport_ident STRING NOT NULL,
  destination_airport_ident STRING NOT NULL,
  origin_iata STRING,
  destination_iata STRING,
  window_hour TIMESTAMP NOT NULL,
  total_flights INT64 NOT NULL,
  avg_duration_minutes FLOAT64,
  min_duration_minutes FLOAT64,
  max_duration_minutes FLOAT64,
  avg_max_altitude_m FLOAT64,
  computed_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(window_hour)
CLUSTER BY origin_airport_ident, destination_airport_ident
OPTIONS (
  partition_expiration_days = 365.0,
  description = "Hourly aggregate statistics for airline routes"
);

