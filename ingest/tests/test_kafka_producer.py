"""Unit tests for KafkaPingPublisher."""

import json
from unittest.mock import MagicMock
import unittest

from ingest.config import IngestionConfig
from ingest.kafka_producer import KafkaPingPublisher
from ingest.models import NormalizedFlightPing


class TestKafkaPingPublisher(unittest.TestCase):
    """Test suite for Kafka flight telemetry producer."""

    def setUp(self):
        self.config = IngestionConfig(
            kafka_bootstrap_servers="localhost:9092",
            kafka_topic="raw-flight-pings",
            kafka_deadletter_topic="deadletter-pings",
        )
        self.sample_ping = NormalizedFlightPing(
            message_id="ping-test-123",
            ingest_timestamp="2026-11-08T12:00:00Z",
            event_timestamp="2026-11-08T11:59:50Z",
            event_time_epoch=1731067190,
            icao24="40001f",
            callsign="BAW123",
            origin_country="United Kingdom",
            longitude=-0.45,
            latitude=51.47,
            baro_altitude=3500.0,
            on_ground=False,
            velocity=180.5,
            true_track=270.0,
            vertical_rate=2.5,
            geo_altitude=3520.0,
            squawk="7000",
            spi=False,
            position_source=0,
        )

    def test_dry_run_mode(self):
        """Dry run mode should count published pings without raising errors."""
        publisher = KafkaPingPublisher(self.config, dry_run=True)
        count = publisher.publish_pings([self.sample_ping])
        self.assertEqual(count, 1)
        self.assertEqual(publisher.published_count, 1)

    def test_publish_pings_keyed_by_icao24(self):
        """Messages sent to Kafka must be keyed by aircraft icao24."""
        mock_producer = MagicMock()
        publisher = KafkaPingPublisher(self.config, producer_client=mock_producer, dry_run=False)

        count = publisher.publish_pings([self.sample_ping])
        self.assertEqual(count, 1)
        self.assertEqual(publisher.published_count, 1)

        # Verify producer.send was called with topic, key=icao24, and payload
        mock_producer.send.assert_called_once()
        args, kwargs = mock_producer.send.call_args
        self.assertEqual(args[0], "raw-flight-pings")
        self.assertEqual(kwargs["key"], "40001f")
        self.assertEqual(kwargs["value"]["callsign"], "BAW123")
        self.assertEqual(kwargs["value"]["latitude"], 51.47)
        mock_producer.flush.assert_called_once()

    def test_publish_deadletter(self):
        """Malformed records must be routed to the deadletter Kafka topic."""
        mock_producer = MagicMock()
        publisher = KafkaPingPublisher(self.config, producer_client=mock_producer, dry_run=False)

        bad_record = {"raw": "invalid_vector", "error": "Missing coordinates"}
        count = publisher.publish_deadletter([bad_record])
        self.assertEqual(count, 1)
        self.assertEqual(publisher.deadletter_count, 1)

        mock_producer.send.assert_called_once_with(
            "deadletter-pings",
            key="deadletter",
            value=bad_record,
        )


if __name__ == "__main__":
    unittest.main()
