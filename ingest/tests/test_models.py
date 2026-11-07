"""Unit tests for OpenSky state vector parsing and normalization."""

from datetime import datetime, timezone
import json
import unittest

from ingest.models import NormalizedFlightPing, parse_state_vector


class TestFlightModels(unittest.TestCase):
    """Test state vector decoding, coordinate checking, and timestamp mapping."""

    def setUp(self):
        self.ref_time = 1700000000
        self.fixed_dt = datetime(2026, 11, 7, 12, 0, 0, tzinfo=timezone.utc)

    def test_parse_valid_state_vector(self):
        """Verify normal complete ADS-B state vector."""
        raw_vector = [
            "484163",             # 0: icao24
            "KLM123  ",           # 1: callsign with whitespace
            "Netherlands",        # 2: origin_country
            1699999990,           # 3: time_position
            1699999995,           # 4: last_contact
            4.7638,               # 5: longitude
            52.3086,              # 6: latitude
            2450.0,               # 7: baro_altitude
            False,                # 8: on_ground
            120.5,                # 9: velocity
            185.0,                # 10: true_track
            -5.2,                 # 11: vertical_rate
            None,                 # 12: sensors
            2480.0,               # 13: geo_altitude
            "1000",               # 14: squawk
            False,                # 15: spi
            0,                    # 16: position_source
            0,                    # 17: category
        ]

        ping, err = parse_state_vector(raw_vector, response_time=self.ref_time, ingest_dt=self.fixed_dt)

        self.assertIsNone(err)
        self.assertIsNotNone(ping)
        assert ping is not None
        self.assertEqual(ping.icao24, "484163")
        self.assertEqual(ping.callsign, "KLM123")  # Whitespace stripped
        self.assertEqual(ping.longitude, 4.7638)
        self.assertEqual(ping.latitude, 52.3086)
        self.assertEqual(ping.baro_altitude, 2450.0)
        self.assertFalse(ping.on_ground)
        self.assertEqual(ping.velocity, 120.5)
        self.assertEqual(ping.true_track, 185.0)
        self.assertEqual(ping.vertical_rate, -5.2)
        self.assertEqual(ping.event_time_epoch, 1699999990)
        self.assertEqual(ping.ingest_timestamp, "2026-11-07T12:00:00+00:00")

        # Verify JSON serialization
        payload = json.loads(ping.to_json())
        self.assertEqual(payload["icao24"], "484163")
        self.assertEqual(payload["callsign"], "KLM123")

        # Verify Pub/Sub attributes
        attrs = ping.get_pubsub_attributes()
        self.assertEqual(attrs["icao24"], "484163")
        self.assertEqual(attrs["on_ground"], "false")

    def test_missing_coordinates_routes_to_deadletter(self):
        """Pings with null latitude/longitude should fail parsing and return error details."""
        raw_vector = [
            "3c4a22", "DLH456", "Germany", 1699999990, 1699999995,
            None, None, 10000.0, False, 220.0, 90.0, 0.0, None, 10020.0, "2000", False, 0
        ]
        ping, err = parse_state_vector(raw_vector, response_time=self.ref_time)
        self.assertIsNone(ping)
        self.assertIsNotNone(err)
        assert err is not None
        self.assertEqual(err["reason"], "missing_coordinates")
        self.assertEqual(err["icao24"], "3c4a22")

    def test_invalid_vector_shape_routes_to_deadletter(self):
        """Malformed or short vectors should route to deadletter."""
        short_vector = ["484163", "KLM123"]
        ping, err = parse_state_vector(short_vector, response_time=self.ref_time)
        self.assertIsNone(ping)
        self.assertIsNotNone(err)
        assert err is not None
        self.assertEqual(err["reason"], "invalid_vector_shape")

    def test_coordinates_out_of_bounds(self):
        """Coordinates outside [-90, 90] lat or [-180, 180] lon should be rejected."""
        invalid_lat_vector = [
            "484163", "KLM123", "Netherlands", 1699999990, 1699999995,
            4.0, 95.0, 1000.0, False, 100.0, 0.0, 0.0, None, 1000.0, None, False, 0
        ]
        ping, err = parse_state_vector(invalid_lat_vector, response_time=self.ref_time)
        self.assertIsNone(ping)
        self.assertIsNotNone(err)
        assert err is not None
        self.assertEqual(err["reason"], "coordinates_out_of_bounds")

    def test_fallback_timestamp_resolution(self):
        """When time_position is None, fall back to last_contact or response_time."""
        vector_no_time_pos = [
            "484163", "KLM123", "Netherlands", None, 1699999950,
            4.76, 52.30, 2000.0, False, 150.0, 180.0, 0.0, None, 2000.0, None, False, 0
        ]
        ping, err = parse_state_vector(vector_no_time_pos, response_time=self.ref_time)
        self.assertIsNone(err)
        self.assertIsNotNone(ping)
        assert ping is not None
        self.assertEqual(ping.event_time_epoch, 1699999950)


if __name__ == "__main__":
    unittest.main()
