"""Unit tests for Pub/Sub batch publisher."""

import unittest
from unittest.mock import MagicMock

from ingest.config import IngestionConfig
from ingest.models import NormalizedFlightPing
from ingest.publisher import PubSubPingPublisher


class TestPubSubPublisher(unittest.TestCase):
    """Test message batching, dead-letter routing, and dry-run execution."""

    def setUp(self):
        self.config = IngestionConfig(
            gcp_project_id="test-project",
            pubsub_topic_id="raw-flight-pings",
            deadletter_topic_id="deadletter-pings",
            batch_size=10,
        )
        self.sample_ping = NormalizedFlightPing(
            message_id="msg-123",
            ingest_timestamp="2026-11-07T12:00:00Z",
            event_timestamp="2026-11-07T11:59:55Z",
            event_time_epoch=1700000000,
            icao24="484163",
            callsign="KLM123",
            origin_country="Netherlands",
            longitude=4.76,
            latitude=52.30,
            baro_altitude=2400.0,
            on_ground=False,
            velocity=120.0,
            true_track=180.0,
            vertical_rate=-5.0,
            geo_altitude=2420.0,
            squawk="1000",
            spi=False,
            position_source=0,
        )

    def test_dry_run_publishing(self):
        """In dry-run mode, publishing increments counters without calling external APIs."""
        publisher = PubSubPingPublisher(self.config, dry_run=True)
        published = publisher.publish_pings([self.sample_ping, self.sample_ping])
        self.assertEqual(published, 2)
        self.assertEqual(publisher.published_count, 2)

        dlq_published = publisher.publish_deadletter([{"reason": "test_error"}])
        self.assertEqual(dlq_published, 1)
        self.assertEqual(publisher.deadletter_count, 1)

    def test_mock_client_publish_with_attributes(self):
        """Verify publisher passes JSON bytes and routing attributes to PubSub client."""
        mock_client = MagicMock()
        mock_future = MagicMock()
        mock_future.result.return_value = "msg-server-id-1"
        mock_client.publish.return_value = mock_future
        mock_client.topic_path.side_effect = lambda p, t: f"projects/{p}/topics/{t}"

        publisher = PubSubPingPublisher(self.config, publisher_client=mock_client, dry_run=False)
        published = publisher.publish_pings([self.sample_ping])

        self.assertEqual(published, 1)
        self.assertEqual(mock_client.publish.call_count, 1)

        # Inspect call kwargs
        args, kwargs = mock_client.publish.call_args
        self.assertEqual(args[0], "projects/test-project/topics/raw-flight-pings")
        self.assertIn(b"484163", kwargs["data"])
        self.assertEqual(kwargs["icao24"], "484163")
        self.assertEqual(kwargs["on_ground"], "false")

    def test_deadletter_publishing(self):
        """Verify deadletter records are published with reason attribute."""
        mock_client = MagicMock()
        mock_future = MagicMock()
        mock_future.result.return_value = "dlq-server-id-1"
        mock_client.publish.return_value = mock_future
        mock_client.topic_path.side_effect = lambda p, t: f"projects/{p}/topics/{t}"

        publisher = PubSubPingPublisher(self.config, publisher_client=mock_client, dry_run=False)
        dlq_published = publisher.publish_deadletter([{"reason": "missing_coordinates", "raw": "bad"}])

        self.assertEqual(dlq_published, 1)
        self.assertEqual(mock_client.publish.call_count, 1)
        args, kwargs = mock_client.publish.call_args
        self.assertEqual(args[0], "projects/test-project/topics/deadletter-pings")
        self.assertEqual(kwargs["reason"], "missing_coordinates")


if __name__ == "__main__":
    unittest.main()
