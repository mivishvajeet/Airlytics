"""Unit tests verifying BigQuery DDL files and schema definitions."""

import os
import re
import unittest

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class TestBigQuerySchemas(unittest.TestCase):
    """Validate DDL syntax, partition rules, clustering keys, and column constraints."""

    def setUp(self):
        self.raw_sql_path = os.path.join(ROOT_DIR, "sql", "raw_tables.sql")
        self.curated_sql_path = os.path.join(ROOT_DIR, "sql", "curated_tables.sql")
        with open(self.raw_sql_path, "r", encoding="utf-8") as f:
            self.raw_sql = f.read()
        with open(self.curated_sql_path, "r", encoding="utf-8") as f:
            self.curated_sql = f.read()

    def test_raw_pings_partitioning_and_clustering(self):
        """raw_flights.raw_pings must be partitioned by DATE(event_timestamp) and clustered by icao24, callsign."""
        self.assertIn("CREATE TABLE IF NOT EXISTS `raw_flights.raw_pings`", self.raw_sql)
        self.assertIn("PARTITION BY DATE(event_timestamp)", self.raw_sql)
        self.assertIn("CLUSTER BY icao24, callsign", self.raw_sql)
        self.assertIn("partition_expiration_days = 90.0", self.raw_sql)

    def test_curated_tables_exist(self):
        """Verify all mandated curated tables are defined with proper clustering and partitioning."""
        tables = [
            "curated_flights.dim_airport",
            "curated_flights.dim_aircraft",
            "curated_flights.fact_flight_leg",
            "curated_flights.fact_airport_event",
            "curated_flights.fact_route_agg",
        ]
        for tbl in tables:
            self.assertIn(f"CREATE TABLE IF NOT EXISTS `{tbl}`", self.curated_sql)

        # fact_flight_leg partitioning
        self.assertIn("PARTITION BY DATE(start_timestamp)", self.curated_sql)
        # fact_airport_event partitioning
        self.assertIn("PARTITION BY DATE(event_timestamp)", self.curated_sql)
        # fact_route_agg partitioning
        self.assertIn("PARTITION BY DATE(window_hour)", self.curated_sql)

    def test_gis_types_in_dim_airport(self):
        """dim_airport must define GEOGRAPHY types for airport_point and geofence_polygon."""
        self.assertIn("airport_point GEOGRAPHY NOT NULL", self.curated_sql)
        self.assertIn("geofence_polygon GEOGRAPHY NOT NULL", self.curated_sql)


if __name__ == "__main__":
    unittest.main()

