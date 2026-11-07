"""Unit tests for late data detection and filtering."""

import unittest

from dataflow.transforms.late_data_detector import evaluate_lateness


class TestLateDataDetector(unittest.TestCase):
    """Test allowed lateness boundary evaluation and timestamp tracking."""

    def test_on_time_event(self):
        """Event within allowed lateness window (e.g. 2 minutes old vs 10 min window) is on-time."""
        ref_time = 1700001000
        ping = {"icao24": "484163", "event_time_epoch": ref_time - 120}  # 2 mins ago
        is_late, lateness = evaluate_lateness(ping, allowed_lateness_seconds=600.0, reference_epoch=ref_time)

        self.assertFalse(is_late)
        self.assertEqual(lateness, 120.0)

    def test_late_event_exceeds_threshold(self):
        """Event older than allowed lateness (e.g. 15 minutes old vs 10 min window) is flagged as late."""
        ref_time = 1700001000
        ping = {"icao24": "484163", "event_time_epoch": ref_time - 900}  # 15 mins ago
        is_late, lateness = evaluate_lateness(ping, allowed_lateness_seconds=600.0, reference_epoch=ref_time)

        self.assertTrue(is_late)
        self.assertEqual(lateness, 900.0)


if __name__ == "__main__":
    unittest.main()
