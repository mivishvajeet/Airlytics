"""Unit tests for PySpark Structured Streaming flight sessionization."""

from datetime import datetime
import json
import unittest

from spark.schemas import FLIGHT_PING_SCHEMA, PYSPARK_AVAILABLE

if PYSPARK_AVAILABLE:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import Row
    from spark.streaming_pipeline import (
        get_spark_session,
        parse_and_watermark_pings,
        sessionize_flight_legs,
    )


class TestSparkStreamingPipeline(unittest.TestCase):
    """Test suite for PySpark transformations and sessionization logic."""

    @classmethod
    def setUpClass(cls):
        if not PYSPARK_AVAILABLE:
            return
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("TestSparkAirlytics")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        if PYSPARK_AVAILABLE and hasattr(cls, "spark"):
            cls.spark.stop()

    def test_schema_availability(self):
        """Verify FLIGHT_PING_SCHEMA contains standard expected fields when PySpark is present."""
        if not PYSPARK_AVAILABLE:
            self.skipTest("pyspark is not installed in test environment")

        self.assertIsNotNone(FLIGHT_PING_SCHEMA)
        field_names = [f.name for f in FLIGHT_PING_SCHEMA.fields]
        self.assertIn("message_id", field_names)
        self.assertIn("icao24", field_names)
        self.assertIn("event_timestamp", field_names)
        self.assertIn("latitude", field_names)
        self.assertIn("longitude", field_names)

    def test_sessionize_flight_legs_logic(self):
        """Verify session windowing groups pings within gap into a single flight leg."""
        if not PYSPARK_AVAILABLE:
            self.skipTest("pyspark is not installed in test environment")

        # Create two pings for same aircraft within 10 minutes (gap is 15m)
        rows = [
            Row(
                message_id="m1",
                event_timestamp="2026-11-08 12:00:00",
                icao24="40001f",
                callsign="BAW123",
                origin_country="United Kingdom",
                longitude=-0.45,
                latitude=51.47,
                baro_altitude=2500.0,
                on_ground=False,
                velocity=150.0,
            ),
            Row(
                message_id="m2",
                event_timestamp="2026-11-08 12:08:00",
                icao24="40001f",
                callsign="BAW123",
                origin_country="United Kingdom",
                longitude=-0.25,
                latitude=51.52,
                baro_altitude=3800.0,
                on_ground=False,
                velocity=190.0,
            ),
        ]

        df = self.spark.createDataFrame(rows)
        watermarked_df = parse_and_watermark_pings(df)
        sessionized_df = sessionize_flight_legs(watermarked_df, gap_duration="15 minutes")

        results = sessionized_df.collect()
        self.assertEqual(len(results), 1)

        leg = results[0]
        self.assertEqual(leg.icao24, "40001f")
        self.assertEqual(leg.callsign, "BAW123")
        self.assertEqual(leg.ping_count, 2)
        self.assertEqual(leg.max_altitude_m, 3800.0)
        self.assertAlmostEqual(leg.avg_velocity_ms, 170.0, places=1)
        self.assertAlmostEqual(leg.duration_minutes, 8.0, places=1)
        self.assertTrue(len(leg.flight_leg_id) > 10)


if __name__ == "__main__":
    unittest.main()

