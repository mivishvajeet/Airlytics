"""Unit tests for Dataflow parse and schema validation transform."""

import json
import unittest

from dataflow.transforms.parse_and_validate import parse_and_validate_payload


class TestParseAndValidate(unittest.TestCase):
    """Test JSON decoding, coordinate boundary checks, and dead-letter routing."""

    def test_valid_payload(self):
        """Standard valid message should pass validation with all fields populated."""
        payload = {
            "message_id": "msg-001",
            "icao24": "484163",
            "callsign": "KLM123",
            "latitude": 52.308,
            "longitude": 4.763,
            "baro_altitude": 2500.0,
            "velocity": 120.0,
            "on_ground": False,
            "event_time_epoch": 1700000000,
        }
        raw_bytes = json.dumps(payload).encode("utf-8")
        valid, dlq = parse_and_validate_payload(raw_bytes)

        self.assertIsNone(dlq)
        self.assertIsNotNone(valid)
        assert valid is not None
        self.assertEqual(valid["icao24"], "484163")
        self.assertEqual(valid["latitude"], 52.308)
        self.assertEqual(valid["longitude"], 4.763)
        self.assertEqual(valid["message_id"], "msg-001")

    def test_invalid_json_bytes_routed_to_dlq(self):
        """Malformed JSON bytes should route to DLQ record."""
        raw_bytes = b"NOT_A_JSON_STRING{{{{"
        valid, dlq = parse_and_validate_payload(raw_bytes)

        self.assertIsNone(valid)
        self.assertIsNotNone(dlq)
        assert dlq is not None
        self.assertIn("json_decode_error", dlq["reason"])
        self.assertEqual(dlq["source_topic"], "raw-flight-pings")

    def test_missing_coordinates_routed_to_dlq(self):
        """Payload missing latitude should route to DLQ."""
        payload = {"icao24": "3c4a22", "longitude": 4.76}
        raw_bytes = json.dumps(payload).encode("utf-8")
        valid, dlq = parse_and_validate_payload(raw_bytes)

        self.assertIsNone(valid)
        self.assertIsNotNone(dlq)
        assert dlq is not None
        self.assertEqual(dlq["reason"], "missing_coordinates")

    def test_out_of_bounds_coordinates(self):
        """Latitude > 90.0 should route to DLQ."""
        payload = {"icao24": "3c4a22", "latitude": 95.0, "longitude": 4.76}
        raw_bytes = json.dumps(payload).encode("utf-8")
        valid, dlq = parse_and_validate_payload(raw_bytes)

        self.assertIsNone(valid)
        self.assertIsNotNone(dlq)
        assert dlq is not None
        self.assertEqual(dlq["reason"], "coordinates_out_of_bounds")


if __name__ == "__main__":
    unittest.main()

