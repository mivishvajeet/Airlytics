"""Google Cloud Pub/Sub batch publisher for flight pings and dead-letter payloads."""

from concurrent import futures
import json
import logging
from typing import Any, Callable, Dict, List, Optional
import uuid

from ingest.config import IngestionConfig
from ingest.models import NormalizedFlightPing

logger = logging.getLogger(__name__)

# Optional import of google-cloud-pubsub to support local test runs without GCP SDK installed
try:
    from google.cloud import pubsub_v1
    PUBSUB_AVAILABLE = True
except ImportError:
    pubsub_v1 = None  # type: ignore
    PUBSUB_AVAILABLE = False


class PubSubPingPublisher:
    """Publishes normalized flight pings in high-throughput batches to Cloud Pub/Sub."""

    def __init__(
        self,
        config: IngestionConfig,
        publisher_client: Optional[Any] = None,
        dry_run: bool = False,
    ):
        self.config = config
        self.dry_run = dry_run
        self.published_count = 0
        self.deadletter_count = 0

        if dry_run:
            self.client = None
            self.main_topic_path = f"projects/{config.gcp_project_id}/topics/{config.pubsub_topic_id}"
            self.dlq_topic_path = f"projects/{config.gcp_project_id}/topics/{config.deadletter_topic_id}"
            logger.info("PubSubPingPublisher initialized in DRY_RUN mode (%s)", self.main_topic_path)
        elif publisher_client is not None:
            self.client = publisher_client
            self.main_topic_path = f"projects/{config.gcp_project_id}/topics/{config.pubsub_topic_id}"
            self.dlq_topic_path = f"projects/{config.gcp_project_id}/topics/{config.deadletter_topic_id}"
            logger.info("PubSubPingPublisher initialized with custom publisher client (%s)", self.main_topic_path)
        elif PUBSUB_AVAILABLE:
            batch_settings = pubsub_v1.types.BatchSettings(
                max_messages=config.batch_size,
                max_latency=config.batch_timeout_seconds,
            )
            self.client = pubsub_v1.PublisherClient(batch_settings=batch_settings)
            self.main_topic_path = self.client.topic_path(
                config.gcp_project_id, config.pubsub_topic_id
            )
            self.dlq_topic_path = self.client.topic_path(
                config.gcp_project_id, config.deadletter_topic_id
            )
            logger.info("PubSubPingPublisher connected to topic: %s", self.main_topic_path)
        else:
            self.client = None
            self.main_topic_path = f"projects/{config.gcp_project_id}/topics/{config.pubsub_topic_id}"
            self.dlq_topic_path = f"projects/{config.gcp_project_id}/topics/{config.deadletter_topic_id}"
            logger.info("google-cloud-pubsub not available; operating in mock publisher mode")

    def publish_pings(self, pings: List[NormalizedFlightPing]) -> int:
        """
        Publish a collection of normalized flight pings to the primary topic.

        Returns:
            Number of successfully published messages.
        """
        if not pings:
            return 0

        if self.dry_run or self.client is None:
            self.published_count += len(pings)
            return len(pings)

        publish_futures: List[futures.Future] = []

        for ping in pings:
            data = ping.to_json().encode("utf-8")
            attrs = ping.get_pubsub_attributes()

            future = self.client.publish(
                self.main_topic_path,
                data=data,
                **attrs,
            )
            publish_futures.append(future)

        # Wait for all batch futures with timeout
        succeeded = 0
        for future in publish_futures:
            try:
                future.result(timeout=30.0)
                succeeded += 1
            except Exception as exc:  # pylint: disable=broad-except
                logger.error("Failed to publish ping to Pub/Sub: %s", exc)

        self.published_count += succeeded
        return succeeded

    def publish_deadletter(self, records: List[Dict[str, Any]]) -> int:
        """
        Publish rejected/malformed records to the dead-letter topic for audit and debugging.

        Returns:
            Number of dead-letter records published.
        """
        if not records:
            return 0

        if self.dry_run or self.client is None:
            self.deadletter_count += len(records)
            return len(records)

        dlq_futures: List[futures.Future] = []

        for record in records:
            payload = json.dumps(record).encode("utf-8")
            future = self.client.publish(
                self.dlq_topic_path,
                data=payload,
                reason=record.get("reason", "unknown_error"),
                message_id=str(uuid.uuid4()),
            )
            dlq_futures.append(future)

        succeeded = 0
        for future in dlq_futures:
            try:
                future.result(timeout=15.0)
                succeeded += 1
            except Exception as exc:  # pylint: disable=broad-except
                logger.error("Failed to publish dead-letter record to Pub/Sub: %s", exc)

        self.deadletter_count += succeeded
        return succeeded
