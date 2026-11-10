"""Unit tests for persistent OpenSky snapshot comparison."""

import tempfile
import unittest
from pathlib import Path

from ingest.snapshot_comparator import SnapshotComparator


class TestSnapshotComparator(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.temp_dir.name) / "state.sqlite")
        self.comparator = SnapshotComparator(self.db_path, missing_threshold=3)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_first_snapshot_establishes_baseline_without_events(self):
        result = self.comparator.process_snapshot(["ABC123", "DEF456"], 1700000000)
        self.assertTrue(result.baseline)
        self.assertEqual(result.observed_count, 2)
        self.assertEqual(result.events, [])

    def test_newly_observed_aircraft_is_detected(self):
        self.comparator.process_snapshot(["abc123"], 1700000000)
        result = self.comparator.process_snapshot(["abc123", "def456"], 1700000060)
        self.assertEqual(result.newly_observed_count, 1)
        self.assertEqual(result.events[0]["event_type"], "newly_observed")
        self.assertEqual(result.events[0]["icao24"], "def456")

    def test_disappearance_requires_consecutive_successful_snapshots(self):
        self.comparator.process_snapshot(["abc123"], 1700000000)

        first_miss = self.comparator.process_snapshot([], 1700000060)
        second_miss = self.comparator.process_snapshot([], 1700000120)
        third_miss = self.comparator.process_snapshot([], 1700000180)

        self.assertEqual(first_miss.no_longer_observed_count, 0)
        self.assertEqual(second_miss.no_longer_observed_count, 0)
        self.assertEqual(third_miss.no_longer_observed_count, 1)
        self.assertEqual(third_miss.events[0]["event_type"], "no_longer_observed")

    def test_reappeared_aircraft_is_detected(self):
        self.comparator.process_snapshot(["abc123"], 1700000000)
        for timestamp in (1700000060, 1700000120, 1700000180):
            self.comparator.process_snapshot([], timestamp)

        result = self.comparator.process_snapshot(["abc123"], 1700000240)
        self.assertEqual(result.reappeared_count, 1)
        self.assertEqual(result.events[0]["event_type"], "reappeared")

    def test_state_survives_comparator_restart(self):
        self.comparator.process_snapshot(["abc123"], 1700000000)
        self.comparator = SnapshotComparator(self.db_path, missing_threshold=3)

        result = self.comparator.process_snapshot(["abc123", "def456"], 1700000060)
        self.assertEqual(result.newly_observed_count, 1)

    def test_identifier_normalization_and_duplicate_removal(self):
        self.comparator.process_snapshot(["ABC123"], 1700000000)
        result = self.comparator.process_snapshot(
            [" abc123 ", "ABC123", "def456"], 1700000060
        )
        self.assertEqual(result.observed_count, 2)
        self.assertEqual(result.newly_observed_count, 1)

    def test_events_remain_pending_until_marked_published(self):
        self.comparator.process_snapshot(["abc123"], 1700000000)
        self.comparator.process_snapshot(["abc123", "def456"], 1700000060)

        pending = self.comparator.get_pending_events()
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["event_type"], "newly_observed")

        self.comparator.mark_events_published([pending[0]["event_id"]])
        self.assertEqual(self.comparator.get_pending_events(), [])

    def test_outbox_survives_comparator_restart(self):
        self.comparator.process_snapshot(["abc123"], 1700000000)
        self.comparator.process_snapshot(["abc123", "def456"], 1700000060)
        self.comparator = SnapshotComparator(self.db_path, missing_threshold=3)

        pending = self.comparator.get_pending_events()
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["icao24"], "def456")

    def test_invalid_threshold_is_rejected(self):
        with self.assertRaises(ValueError):
            SnapshotComparator(self.db_path, missing_threshold=0)


if __name__ == "__main__":
    unittest.main()
