"""Unit test verifying dbt project structure, models, sources, and BigQuery GIS syntax."""

import os
import unittest

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DBT_DIR = os.path.join(ROOT_DIR, "dbt_airlytics")


class TestDbtProject(unittest.TestCase):
    """Validate dbt project configurations, SQL models, and schema tests."""

    def test_dbt_project_configuration(self):
        """dbt_project.yml must declare name and model configurations."""
        project_file = os.path.join(DBT_DIR, "dbt_project.yml")
        self.assertTrue(os.path.exists(project_file))
        with open(project_file, "r", encoding="utf-8") as f:
            content = f.read()
        self.assertIn("name: 'airlytics'", content)
        self.assertIn("profile: 'airlytics'", content)
        self.assertIn("models:", content)
        if YAML_AVAILABLE:
            config = yaml.safe_load(content)
            self.assertEqual(config["name"], "airlytics")

    def test_required_dbt_models_exist(self):
        """All staging and marts models must exist."""
        expected_models = [
            "staging/stg_raw_pings.sql",
            "staging/stg_airports.sql",
            "staging/sources.yml",
            "staging/schema.yml",
            "marts/dim_airport.sql",
            "marts/fact_flight_leg.sql",
            "marts/fact_airport_event.sql",
            "marts/fact_route_agg.sql",
            "marts/schema.yml",
        ]
        for rel_path in expected_models:
            full_path = os.path.join(DBT_DIR, "models", rel_path)
            self.assertTrue(os.path.exists(full_path), f"Missing dbt model file: {rel_path}")

    def test_bigquery_gis_usage_in_marts(self):
        """dim_airport and fact_airport_event must utilize BigQuery GIS functions."""
        dim_airport = os.path.join(DBT_DIR, "models", "staging", "stg_airports.sql")
        fact_event = os.path.join(DBT_DIR, "models", "marts", "fact_airport_event.sql")

        with open(dim_airport, "r", encoding="utf-8") as f:
            content = f.read()
        self.assertIn("ST_BUFFER", content)
        self.assertIn("ST_GEOGPOINT", content)

        with open(fact_event, "r", encoding="utf-8") as f:
            content = f.read()
        self.assertIn("ST_CONTAINS", content)
        self.assertIn("ST_DISTANCE", content)

    def test_custom_data_quality_tests_exist(self):
        """Custom singular tests must be present in tests/ directory."""
        expected_tests = [
            "assert_deduplication_rate.sql",
            "assert_valid_flight_duration.sql",
            "assert_airspace_freshness.sql",
        ]
        for t in expected_tests:
            full_path = os.path.join(DBT_DIR, "tests", t)
            self.assertTrue(os.path.exists(full_path), f"Missing dbt test file: {t}")


if __name__ == "__main__":
    unittest.main()
