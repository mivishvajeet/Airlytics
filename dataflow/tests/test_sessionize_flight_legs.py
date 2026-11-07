"""Unit tests for flight leg sessionization and trajectory aggregation."""

import unittest

from dataflow.transforms.sessionize_flight_legs import (
    FlightLegAccumulator,
    sessionize_pings,
)


class TestSessionizeFlightLegs(unittest.TestCase):
    """Test sessionization with synthetic flight trajectories and gap-based splitting."""

    def test_single_continuous_flight_leg(self):
        """Simulate a single continuous flight trajectory (climb -> cruise -> descent)."""
        base_epoch = 1700000000
        synthetic_pings = [
            # Takeoff from Amsterdam (EHAM: 52.308, 4.763)
            {"icao24": "484163", "callsign": "KLM123", "event_time_epoch": base_epoch + 0, "latitude": 52.31, "longitude": 4.76, "baro_altitude": 100.0, "velocity": 80.0},
            {"icao24": "484163", "callsign": "KLM123", "event_time_epoch": base_epoch + 120, "latitude": 52.10, "longitude": 4.10, "baro_altitude": 3000.0, "velocity": 150.0},
            {"icao24": "484163", "callsign": "KLM123", "event_time_epoch": base_epoch + 600, "latitude": 51.80, "longitude": 2.50, "baro_altitude": 8500.0, "velocity": 210.0},
            {"icao24": "484163", "callsign": "KLM123", "event_time_epoch": base_epoch + 1200, "latitude": 51.55, "longitude": 0.80, "baro_altitude": 5000.0, "velocity": 160.0},
            # Landing near London Heathrow (EGLL: 51.470, -0.461)
            {"icao24": "484163", "callsign": "KLM123", "event_time_epoch": base_epoch + 1800, "latitude": 51.47, "longitude": -0.46, "baro_altitude": 50.0, "velocity": 70.0},
        ]

        legs = sessionize_pings(synthetic_pings, gap_seconds=900, min_pings=2)

        self.assertEqual(len(legs), 1)
        leg = legs[0]
        self.assertEqual(leg["icao24"], "484163")
        self.assertEqual(leg["callsign"], "KLM123")
        self.assertEqual(leg["total_pings"], 5)
        self.assertEqual(leg["duration_minutes"], 30.0)  # 1800s = 30min
        self.assertEqual(leg["max_altitude_m"], 8500.0)
        self.assertEqual(leg["max_velocity_mps"], 210.0)
        self.assertEqual(leg["origin_lat"], 52.31)
        self.assertEqual(leg["dest_lat"], 51.47)
        self.assertIsNotNone(leg["flight_leg_id"])

    def test_gap_splits_into_two_distinct_flight_legs(self):
        """Simulate two flights separated by a 45-minute turnaround gap (gap > 15 minutes)."""
        base_epoch = 1700000000
        # Flight 1: Amsterdam to London (pings 60s apart)
        flight_1 = [
            {"icao24": "484163", "callsign": "KLM123", "event_time_epoch": base_epoch + 0, "latitude": 52.31, "longitude": 4.76, "baro_altitude": 200.0, "velocity": 90.0},
            {"icao24": "484163", "callsign": "KLM123", "event_time_epoch": base_epoch + 300, "latitude": 51.90, "longitude": 2.10, "baro_altitude": 6000.0, "velocity": 180.0},
            {"icao24": "484163", "callsign": "KLM123", "event_time_epoch": base_epoch + 600, "latitude": 51.47, "longitude": -0.46, "baro_altitude": 100.0, "velocity": 75.0},
        ]

        # 45 minutes (2700s) silence gap at London Heathrow gate
        flight_2_start = base_epoch + 600 + 2700
        # Flight 2: London to Frankfurt (pings 60s apart)
        flight_2 = [
            {"icao24": "484163", "callsign": "KLM124", "event_time_epoch": flight_2_start + 0, "latitude": 51.47, "longitude": -0.46, "baro_altitude": 150.0, "velocity": 85.0},
            {"icao24": "484163", "callsign": "KLM124", "event_time_epoch": flight_2_start + 400, "latitude": 50.80, "longitude": 4.50, "baro_altitude": 7000.0, "velocity": 190.0},
            {"icao24": "484163", "callsign": "KLM124", "event_time_epoch": flight_2_start + 800, "latitude": 50.03, "longitude": 8.57, "baro_altitude": 200.0, "velocity": 80.0},
        ]

        all_pings = flight_1 + flight_2
        legs = sessionize_pings(all_pings, gap_seconds=900, min_pings=2)

        self.assertEqual(len(legs), 2)
        # Leg 1: AMS -> LHR
        self.assertEqual(legs[0]["callsign"], "KLM123")
        self.assertEqual(legs[0]["origin_lat"], 52.31)
        self.assertEqual(legs[0]["dest_lat"], 51.47)
        self.assertEqual(legs[0]["total_pings"], 3)

        # Leg 2: LHR -> FRA
        self.assertEqual(legs[1]["callsign"], "KLM124")
        self.assertEqual(legs[1]["origin_lat"], 51.47)
        self.assertEqual(legs[1]["dest_lat"], 50.03)
        self.assertEqual(legs[1]["total_pings"], 3)

        # IDs must be different and deterministic
        self.assertNotEqual(legs[0]["flight_leg_id"], legs[1]["flight_leg_id"])

    def test_single_ping_session_filtered_out(self):
        """Sessions with fewer than min_pings should be filtered out to eliminate spurious blips."""
        single_ping = [{"icao24": "484163", "callsign": "KLM123", "event_time_epoch": 1700000000, "latitude": 52.0, "longitude": 4.0}]
        legs = sessionize_pings(single_ping, gap_seconds=900, min_pings=2)
        self.assertEqual(len(legs), 0)


if __name__ == "__main__":
    unittest.main()
