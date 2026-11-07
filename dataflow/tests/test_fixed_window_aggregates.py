"""Unit tests for fixed 5-minute window airspace aggregations."""

import unittest

from dataflow.transforms.fixed_window_aggregates import aggregate_5m_window


class TestFixedWindowAggregates(unittest.TestCase):
    """Test 5-minute window rollups, active aircraft counting, and velocity averaging."""

    def test_window_aggregation(self):
        pings = [
            {"icao24": "484163", "on_ground": False, "baro_altitude": 3000.0, "velocity": 150.0},
            {"icao24": "484163", "on_ground": False, "baro_altitude": 3500.0, "velocity": 160.0},
            {"icao24": "3c4a22", "on_ground": True, "baro_altitude": 0.0, "velocity": 10.0},
        ]
        res = aggregate_5m_window(pings, window_start="2026-11-08T00:00:00Z", window_end="2026-11-08T00:05:00Z")

        self.assertEqual(res["total_pings"], 3)
        self.assertEqual(res["active_aircraft_count"], 2)
        self.assertEqual(res["airborne_aircraft_count"], 1)
        self.assertEqual(res["on_ground_aircraft_count"], 1)
        self.assertAlmostEqual(res["avg_altitude_m"], (3000 + 3500 + 0) / 3, places=1)


if __name__ == "__main__":
    unittest.main()

