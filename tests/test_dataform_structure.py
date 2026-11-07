"""Unit test verifying Dataform directory structure, SQLX models, and GIS syntax."""

import os
import re
import unittest

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATAFORM_DIR = os.path.join(ROOT_DIR, "dataform")


class TestDataformProject(unittest.TestCase):
    """Validate Dataform project configuration and SQLX definitions."""

    def test_workflow_settings(self):
        """workflow_settings.yaml must be present and declare defaultProject and location."""
        settings_path = os.path.join(DATAFORM_DIR, "workflow_settings.yaml")
        self.assertTrue(os.path.exists(settings_path))
        with open(settings_path, "r", encoding="utf-8") as f:
            content = f.read()
        self.assertIn("defaultProject:", content)
        self.assertIn("defaultLocation:", content)
        self.assertIn("defaultDataset:", content)

    def test_required_sqlx_models_exist(self):
        """All mandated dimensional and fact models must be present in definitions/."""
        expected_files = [
            "dim_airport.sqlx",
            "dim_aircraft.sqlx",
            "fact_flight_leg.sqlx",
            "fact_airport_event.sqlx",
            "fact_route_agg.sqlx",
            "assertions/assert_deduplication_rate.sqlx",
            "assertions/assert_valid_flight_duration.sqlx",
            "assertions/assert_airspace_freshness.sqlx",
        ]
        for rel_path in expected_files:
            full_path = os.path.join(DATAFORM_DIR, "definitions", rel_path)
            self.assertTrue(os.path.exists(full_path), f"Missing expected SQLX file: {rel_path}")

    def test_bigquery_gis_functions_used(self):
        """fact_airport_event and fact_flight_leg must leverage BigQuery GIS ST_* functions."""
        airport_event_path = os.path.join(DATAFORM_DIR, "definitions", "fact_airport_event.sqlx")
        flight_leg_path = os.path.join(DATAFORM_DIR, "definitions", "fact_flight_leg.sqlx")

        with open(airport_event_path, "r", encoding="utf-8") as f:
            content = f.read()
        self.assertIn("ST_CONTAINS", content)
        self.assertIn("ST_DISTANCE", content)

        with open(flight_leg_path, "r", encoding="utf-8") as f:
            content = f.read()
        self.assertIn("ST_DISTANCE", content)
        self.assertIn("ST_GEOGPOINT", content)

    def test_assertions_syntax(self):
        """Assertions must declare type: assertion and valid fail conditions."""
        dedupe_path = os.path.join(DATAFORM_DIR, "definitions", "assertions", "assert_deduplication_rate.sqlx")
        freshness_path = os.path.join(DATAFORM_DIR, "definitions", "assertions", "assert_airspace_freshness.sqlx")

        with open(dedupe_path, "r", encoding="utf-8") as f:
            content = f.read()
        self.assertIn('type: "assertion"', content)
        self.assertIn("0.001", content)  # 0.1% threshold

        with open(freshness_path, "r", encoding="utf-8") as f:
            content = f.read()
        self.assertIn('type: "assertion"', content)
        self.assertIn("10", content)  # 10 minute threshold


if __name__ == "__main__":
    unittest.main()
