"""Seed BigQuery with live OpenSky flights and airport geofence reference data."""

import argparse
from datetime import datetime, timezone
import json
import logging
import sys
import time

try:
    from google.cloud import bigquery
    BIGQUERY_AVAILABLE = True
except ImportError:
    bigquery = None  # type: ignore
    BIGQUERY_AVAILABLE = False

from ingest.config import IngestionConfig
from ingest.poller import OpenSkyPoller

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed-bigquery")

# Curated reference airports
AIRPORTS_DATA = [
    ("EHAM", "Amsterdam Airport Schiphol", 52.308601, 4.76389, -11, "NL", "AMS"),
    ("EGLL", "London Heathrow Airport", 51.4706, -0.461941, 83, "GB", "LHR"),
    ("EDDF", "Frankfurt am Main Airport", 50.033333, 8.570556, 364, "DE", "FRA"),
    ("LFPG", "Charles de Gaulle International Airport", 49.012798, 2.55, 392, "FR", "CDG"),
    ("EDDM", "Munich Airport", 48.353799, 11.786, 1487, "DE", "MUC"),
    ("LSZH", "Zurich Airport", 47.464699, 8.54917, 1416, "CH", "ZRH"),
    ("EBBR", "Brussels Airport", 50.901402, 4.48444, 184, "BE", "BRU"),
    ("LEMD", "Adolfo Suárez Madrid-Barajas Airport", 40.471926, -3.56264, 1998, "ES", "MAD"),
    ("KJFK", "John F. Kennedy International Airport", 40.639801, -73.7789, 13, "US", "JFK"),
]


def populate_airports(client: Any, project_id: str, dataset: str) -> None:
    """Create and seed dim_airport with GIS GEOGRAPHY points and 5km buffer polygons."""
    table_id = f"{project_id}.{dataset}.dim_airport"
    logger.info("Seeding airport dimensions into `%s`...", table_id)

    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{table_id}` (
      ident STRING NOT NULL,
      name STRING NOT NULL,
      latitude_deg FLOAT64 NOT NULL,
      longitude_deg FLOAT64 NOT NULL,
      elevation_ft INT64,
      iso_country STRING,
      iata_code STRING,
      airport_point GEOGRAPHY NOT NULL,
      geofence_polygon GEOGRAPHY NOT NULL
    )
    CLUSTER BY ident, iata_code;
    """
    client.query(ddl).result()

    # Clear and insert fresh reference data
    client.query(f"DELETE FROM `{table_id}` WHERE TRUE").result()

    insert_rows = []
    for ident, name, lat, lon, elev, country, iata in AIRPORTS_DATA:
        insert_rows.append(
            f"('{ident}', '{name}', {lat}, {lon}, {elev}, '{country}', '{iata}', "
            f"ST_GEOGPOINT({lon}, {lat}), ST_BUFFER(ST_GEOGPOINT({lon}, {lat}), 5000.0))"
        )

    insert_sql = f"""
    INSERT INTO `{table_id}` (ident, name, latitude_deg, longitude_deg, elevation_ft, iso_country, iata_code, airport_point, geofence_polygon)
    VALUES {', '.join(insert_rows)};
    """
    client.query(insert_sql).result()
    logger.info("Successfully seeded %d airports with 5km GIS geofences into `%s`!", len(AIRPORTS_DATA), table_id)


def stream_live_pings_to_bigquery(client: Any, project_id: str, dataset: str) -> None:
    """Fetch live aircraft states from OpenSky and stream them directly into raw_pings."""
    table_id = f"{project_id}.{dataset}.raw_pings"
    logger.info("Fetching real-time aircraft positions from OpenSky Network...")

    config = IngestionConfig(
        poll_interval_seconds=5.0,
        bbox_lamin=47.5,
        bbox_lomin=-2.0,
        bbox_lamax=54.5,
        bbox_lomax=12.0,
    )
    poller = OpenSkyPoller(config)
    result = poller.poll_once()

    if not result.valid_pings:
        logger.warning("No live aircraft returned from OpenSky (possibly throttled).")
        return

    logger.info("Received %d live aircraft state vectors. Streaming into `%s`...", len(result.valid_pings), table_id)

    rows_to_insert = []
    now_iso = datetime.now(timezone.utc).isoformat()
    for p in result.valid_pings[:150]:  # Stream first 150 live aircraft
        rows_to_insert.append({
            "message_id": p.ping_id,
            "ingest_timestamp": now_iso,
            "event_timestamp": datetime.fromtimestamp(p.time_position or p.event_timestamp, tz=timezone.utc).isoformat(),
            "event_time_epoch": p.time_position or p.event_timestamp,
            "icao24": p.icao24,
            "callsign": p.callsign or "UNKNOWN",
            "origin_country": p.origin_country,
            "longitude": p.longitude,
            "latitude": p.latitude,
            "baro_altitude": p.altitude_baro_m,
            "on_ground": p.on_ground,
            "velocity": p.velocity_ms,
            "true_track": p.heading_deg,
            "vertical_rate": p.vertical_rate_ms,
            "geo_altitude": p.altitude_geom_m,
            "squawk": p.squawk,
            "spi": p.spi,
            "position_source": p.position_source,
        })

    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        logger.error("Failed to insert some rows: %s", errors[:2])
    else:
        logger.info("Successfully streamed %d live aircraft rows into `%s`!", len(rows_to_insert), table_id)


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed BigQuery with live flight telemetry and airport geofences")
    parser.add_argument("--project-id", default="airlytics-dev", help="GCP Project ID")
    parser.add_argument("--env", default="dev", help="Environment suffix (dev/prod)")
    args = parser.parse_args()

    if not BIGQUERY_AVAILABLE:
        print("ERROR: google-cloud-bigquery is required. Run: pip install google-cloud-bigquery")
        sys.exit(1)

    client = bigquery.Client(project=args.project_id)
    raw_dataset = f"raw_flights_{args.env}"
    curated_dataset = f"curated_flights_{args.env}"

    populate_airports(client, args.project_id, curated_dataset)
    stream_live_pings_to_bigquery(client, args.project_id, raw_dataset)
    logger.info("All data populated! Refresh your BigQuery console to inspect tables and preview live rows.")


if __name__ == "__main__":
    main()
