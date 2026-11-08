"""Apache Kafka producer for streaming real-time flight pings and dead-letter records."""

from dataclasses import asdict
import json
import logging
import time
from typing import Any, Dict, List, Optional

from ingest.config import IngestionConfig
from ingest.models import NormalizedFlightPing

logger = logging.getLogger(__name__)

# Optional import of kafka-python / confluent-kafka to support mock environments
try:
    from kafka import KafkaProducer  # type: ignore
    KAFKA_AVAILABLE = True
except ImportError:
    KafkaProducer = None  # type: ignore
    KAFKA_AVAILABLE = False


class KafkaPingPublisher:
    """Publishes normalized flight pings partitioned by aircraft ICAO24 to Apache Kafka."""

    def __init__(
        self,
        config: IngestionConfig,
        producer_client: Optional[Any] = None,
        dry_run: bool = False,
    ):
        self.config = config
        self.dry_run = dry_run
        self.published_count = 0
        self.deadletter_count = 0

        if dry_run or not KAFKA_AVAILABLE:
            self.producer = producer_client
            mode_desc = "DRY_RUN" if dry_run else "MOCK (kafka library not installed)"
            logger.info("KafkaPingPublisher initialized in %s mode (Target: %s)", mode_desc, config.kafka_bootstrap_servers)
        elif producer_client is not None:
            self.producer = producer_client
            logger.info("KafkaPingPublisher initialized with custom Kafka producer client")
        else:
            self.producer = KafkaProducer(
                bootstrap_servers=config.kafka_bootstrap_servers.split(","),
                key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                acks="all",
                retries=3,
                linger_ms=10,
                batch_size=16384,
            )
            logger.info(
                "KafkaPingPublisher connected to bootstrap servers: %s (Topic: %s)",
                config.kafka_bootstrap_servers,
                config.kafka_topic,
            )

    def publish_pings(self, pings: List[NormalizedFlightPing]) -> int:
        """Publish a batch of normalized flight pings keyed by icao24."""
        if not pings:
            return 0

        success_count = 0
        for ping in pings:
            payload = asdict(ping)
            key = ping.icao24

            if self.dry_run or self.producer is None:
                success_count += 1
                continue

            try:
                # Key by icao24 ensures all pings from the same aircraft map to the same Kafka partition
                if hasattr(self.producer, "send"):
                    self.producer.send(
                        self.config.kafka_topic,
                        key=key,
                        value=payload,
                        headers=[
                            ("event_timestamp", str(ping.event_timestamp).encode("utf-8")),
                            ("icao24", key.encode("utf-8")),
                        ],
                    )
                elif hasattr(self.producer, "produce"):
                    self.producer.produce(
                        self.config.kafka_topic,
                        key=key.encode("utf-8"),
                        value=json.dumps(payload, default=str).encode("utf-8"),
                    )
                success_count += 1
            except Exception as e:
                logger.error("Failed to publish ping %s to Kafka: %s", ping.message_id, e)

        if self.producer and hasattr(self.producer, "flush"):
            self.producer.flush()

        self.published_count += success_count
        return success_count

    def publish_deadletter(self, deadletter_records: List[Dict[str, Any]]) -> int:
        """Publish malformed or unpositioned records to the dead-letter Kafka topic."""
        if not deadletter_records:
            return 0

        dlq_count = 0
        for record in deadletter_records:
            if self.dry_run or self.producer is None:
                dlq_count += 1
                continue

            try:
                if hasattr(self.producer, "send"):
                    self.producer.send(
                        self.config.kafka_deadletter_topic,
                        key="deadletter",
                        value=record,
                    )
                elif hasattr(self.producer, "produce"):
                    self.producer.produce(
                        self.config.kafka_deadletter_topic,
                        key=b"deadletter",
                        value=json.dumps(record, default=str).encode("utf-8"),
                    )
                dlq_count += 1
            except Exception as e:
                logger.error("Failed to route dead-letter record to Kafka: %s", e)

        if self.producer and hasattr(self.producer, "flush"):
            self.producer.flush()

        self.deadletter_count += dlq_count
        return dlq_count
