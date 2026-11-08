"""Main entrypoint for Airlytics OpenSky Ingestion Service on Cloud Run."""

import logging
import os
import signal
import sys
import time
from typing import Any

from ingest.config import IngestionConfig
from ingest.kafka_producer import KafkaPingPublisher
from ingest.poller import OpenSkyPoller

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s", "level":"%(levelname)s", "module":"%(name)s", "message":"%(message)s"}',
)
logger = logging.getLogger("airlytics-ingestion")


class IngestionRunner:
    """Manages the polling lifecycle, graceful shutdown, and metric tracking."""

    def __init__(self, config: IngestionConfig, dry_run: bool = False):
        self.config = config
        self.dry_run = dry_run
        self.poller = OpenSkyPoller(config)
        if config.streaming_sink == "kafka":
            self.publisher = KafkaPingPublisher(config, dry_run=dry_run)
        else:
            raise ValueError('Airlytics is local Kafka-only. Set STREAMING_SINK=kafka; cloud Pub/Sub is not part of the free local runtime.')
        self._running = True
        self._setup_signals()

    def _setup_signals(self) -> None:
        """Register signal handlers for SIGTERM (Cloud Run shutdown) and SIGINT."""
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

    def _handle_shutdown(self, signum: int, frame: Any) -> None:
        logger.info("Received signal %d. Initiating graceful shutdown...", signum)
        self._running = False

    def run(self) -> int:
        """Execute the polling loop."""
        logger.info(
            "Starting OpenSky Ingestion Service. Target: %s, Topic: %s, Interval: %.1fs, BBox: %s",
            self.config.opensky_url,
            self.config.kafka_topic,
            self.config.poll_interval_seconds,
            "Enabled" if self.config.has_bbox else "Global",
        )

        cycle_count = 0
        total_pings_published = 0
        total_deadletter_published = 0
        start_time = time.time()

        while self._running:
            cycle_start = time.perf_counter()
            cycle_count += 1

            poll_result = self.poller.poll_cycle()

            if poll_result.valid_pings:
                published = self.publisher.publish_pings(poll_result.valid_pings)
                total_pings_published += published
            else:
                published = 0

            if poll_result.deadletter_records:
                dlq_published = self.publisher.publish_deadletter(poll_result.deadletter_records)
                total_deadletter_published += dlq_published
            else:
                dlq_published = 0

            logger.info(
                "Cycle %d completed in %.2fms (HTTP %d). States: %d | Published: %d | DLQ: %d | Total published: %d",
                cycle_count,
                poll_result.api_latency_ms,
                poll_result.status_code,
                poll_result.total_states_received,
                published,
                dlq_published,
                total_pings_published,
            )

            # Check if duration limit reached (for bounded local runs)
            if self.config.run_duration_seconds > 0:
                elapsed = time.time() - start_time
                if elapsed >= self.config.run_duration_seconds:
                    logger.info("Configured run duration reached (%.1fs). Exiting cleanly.", elapsed)
                    break

            # Sleep remaining interval to maintain steady cadence
            elapsed_cycle = time.perf_counter() - cycle_start
            sleep_duration = max(0.0, self.config.poll_interval_seconds - elapsed_cycle)
            if self._running and sleep_duration > 0:
                time.sleep(sleep_duration)

        logger.info(
            "Ingestion terminated. Summary: %d cycles, %d pings published, %d dead-lettered.",
            cycle_count,
            total_pings_published,
            total_deadletter_published,
        )
        return 0


def main() -> int:
    """Application entrypoint."""
    dry_run = os.getenv("DRY_RUN", "false").lower() in ("true", "1", "yes")
    config = IngestionConfig()
    runner = IngestionRunner(config=config, dry_run=dry_run)
    return runner.run()


if __name__ == "__main__":
    sys.exit(main())
