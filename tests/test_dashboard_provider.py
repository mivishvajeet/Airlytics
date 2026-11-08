"""Unit tests for Airlytics dashboard data provider and GIS calculations."""

import unittest

from dashboard.data_provider import (
    DashboardDataProvider,
    find_nearest_airport,
    haversine_km,
    PANDAS_AVAILABLE,
)


class TestDashboardDataProvider(unittest.TestCase):
    """Test suite for dashboard data transformations and calculations."""

    def setUp(self):
        self.provider = DashboardDataProvider()

    def test_haversine_calculation(self):
        """Verify distance between London Heathrow (EGLL) and Paris CDG (LFPG) is ~348km."""
        # EGLL: 51.4700, -0.4543 | LFPG: 49.0097, 2.5479
        dist = haversine_km(51.4700, -0.4543, 49.0097, 2.5479)
        self.assertAlmostEqual(dist, 348.0, delta=15.0)

    def test_find_nearest_airport_match(self):
        """Points right next to Heathrow should resolve to EGLL."""
        res = find_nearest_airport(51.475, -0.460, max_dist_km=10.0)
        self.assertIsNotNone(res)
        self.assertEqual(res[0], "EGLL")
        self.assertLess(res[2], 5.0)

    def test_find_nearest_airport_out_of_range(self):
        """Points in the middle of the Atlantic should resolve to None."""
        res = find_nearest_airport(30.0, -40.0, max_dist_km=10.0)
        self.assertIsNone(res)

    def test_synthetic_pings_structure(self):
        """Ensure synthetic telemetry dataframe/list has expected keys/columns and non-empty records."""
        data = self.provider.get_synthetic_pings(count=50)
        self.assertEqual(len(data), 50)
        expected_cols = [
            "ping_id", "icao24", "callsign", "latitude", "longitude",
            "altitude_baro_m", "velocity_ms", "vertical_rate_ms", "on_ground"
        ]
        sample = data.iloc[0] if PANDAS_AVAILABLE else data[0]
        for col in expected_cols:
            self.assertIn(col, sample)

    def test_curated_flight_legs_derivation(self):
        """Ensure flight legs are derived from pings with valid schemas."""
        pings = self.provider.get_synthetic_pings(count=30)
        legs = self.provider.get_curated_flight_legs(pings)
        self.assertGreater(len(legs), 0)
        sample = legs.iloc[0] if PANDAS_AVAILABLE else legs[0]
        self.assertIn("flight_leg_id", sample)
        self.assertIn("origin_airport", sample)
        self.assertIn("destination_airport", sample)

    def test_route_aggregates_derivation(self):
        """Ensure route aggregation groups and computes statistics accurately."""
        pings = self.provider.get_synthetic_pings(count=40)
        legs = self.provider.get_curated_flight_legs(pings)
        routes = self.provider.get_route_aggregates(legs)
        self.assertGreater(len(routes), 0)
        sample = routes.iloc[0] if PANDAS_AVAILABLE else routes[0]
        self.assertIn("route", sample)
        self.assertIn("flight_count", sample)


if __name__ == "__main__":
    unittest.main()
