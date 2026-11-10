"""Unit tests for OpenSky REST API Poller."""

import unittest
from unittest.mock import MagicMock, patch

from ingest.config import IngestionConfig
from ingest.poller import OpenSkyPoller


class TestOpenSkyPoller(unittest.TestCase):
    """Test API polling, retry logic, rate limit backoff, and cycle outcomes."""

    def setUp(self):
        self.config = IngestionConfig(
            opensky_url="https://opensky-network.org/api/states/all",
            poll_interval_seconds=1.0,
            max_retries=3,
            backoff_factor=1.0,
            request_timeout_seconds=2.0,
            bbox_lamin=45.0,
            bbox_lomin=-5.0,
            bbox_lamax=55.0,
            bbox_lomax=15.0,
        )

    def test_params_with_bounding_box(self):
        """Bounding box parameters should be attached to query params."""
        poller = OpenSkyPoller(self.config)
        params = poller._build_params()
        self.assertEqual(params["lamin"], 45.0)
        self.assertEqual(params["lomin"], -5.0)
        self.assertEqual(params["lamax"], 55.0)
        self.assertEqual(params["lomax"], 15.0)

    def test_params_global_when_bbox_disabled(self):
        """When bbox is None, query params should be empty (global query)."""
        global_config = IngestionConfig(
            bbox_lamin=None, bbox_lomin=None, bbox_lamax=None, bbox_lomax=None
        )
        poller = OpenSkyPoller(global_config)
        params = poller._build_params()
        self.assertEqual(params, {})

    @patch("time.sleep")
    def test_successful_poll_cycle(self, mock_sleep):
        """Verify successful HTTP 200 response correctly parses valid and invalid vectors."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "time": 1700000000,
            "states": [
                # Valid vector
                ["484163", "KLM123", "Netherlands", 1700000000, 1700000000, 4.76, 52.30, 2000.0, False, 120.0, 180.0, 0.0, None, 2000.0, "1000", False, 0],
                # Missing coordinates (deadletter)
                ["3c4a22", "DLH456", "Germany", 1700000000, 1700000000, None, None, 10000.0, False, 200.0, 90.0, 0.0, None, 10000.0, "2000", False, 0],
            ],
        }
        mock_session.get.return_value = mock_response

        poller = OpenSkyPoller(self.config, session=mock_session)
        result = poller.poll_cycle()

        self.assertEqual(result.status_code, 200)
        self.assertEqual(result.total_states_received, 2)
        self.assertEqual(result.observed_icao24s, ["484163", "3c4a22"])
        self.assertEqual(len(result.valid_pings), 1)
        self.assertEqual(result.valid_pings[0].icao24, "484163")
        self.assertEqual(len(result.deadletter_records), 1)
        self.assertEqual(result.deadletter_records[0]["reason"], "missing_coordinates")
        mock_sleep.assert_not_called()

    def test_successful_empty_snapshot_is_not_a_fetch_failure(self):
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"time": 1700000000, "states": []}
        mock_session.get.return_value = mock_response

        poller = OpenSkyPoller(self.config, session=mock_session)
        result = poller.poll_cycle()

        self.assertEqual(result.status_code, 200)
        self.assertEqual(result.total_states_received, 0)
        self.assertEqual(result.observed_icao24s, [])
        self.assertEqual(result.deadletter_records, [])

    def test_malformed_success_payload_is_not_treated_as_empty_snapshot(self):
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"time": 1700000000}
        mock_session.get.return_value = mock_response

        poller = OpenSkyPoller(self.config, session=mock_session)
        result = poller.poll_cycle()

        self.assertEqual(result.status_code, 502)
        self.assertEqual(result.observed_icao24s, [])
        self.assertEqual(result.deadletter_records[0]["reason"], "invalid_api_payload")

    @patch("time.sleep")
    def test_rate_limit_backoff_and_retry(self, mock_sleep):
        """When receiving HTTP 429, poller should back off and retry until success."""
        mock_session = MagicMock()

        resp_429 = MagicMock()
        resp_429.status_code = 429

        resp_200 = MagicMock()
        resp_200.status_code = 200
        resp_200.json.return_value = {
            "time": 1700000000,
            "states": [
                ["484163", "KLM123", "Netherlands", 1700000000, 1700000000, 4.76, 52.30, 2000.0, False, 120.0, 180.0, 0.0, None, 2000.0, "1000", False, 0],
            ],
        }

        mock_session.get.side_effect = [resp_429, resp_200]

        poller = OpenSkyPoller(self.config, session=mock_session)
        result = poller.poll_cycle()

        self.assertEqual(result.status_code, 200)
        self.assertEqual(len(result.valid_pings), 1)
        self.assertEqual(mock_session.get.call_count, 2)
        mock_sleep.assert_called_once()


if __name__ == "__main__":
    unittest.main()
